from django.shortcuts import render
import sqlite3
import os
import time
from django.conf import settings
import json
import csv
import io
from django.http import HttpResponse
import re
import pandas as pd
from datetime import datetime
from urllib.parse import urlencode
from dateutil import parser as date_parser

# jednoduchá in-memory cache pro načtené stránky/tabulky
IMPORT_CACHE = {
    'web': {},   # key: url -> list of DataFrames
    'ote': {}    # key: (date, res) -> list of DataFrames
}
# jednoduchý log do paměti
IMPORT_LOGS = []  # list of dicts: {imported_at, source, source_type, table_name, rows, cols}

# seznam systémových tabulek, které nechceme zobrazovat v UI
EXCLUDED_TABLES = {
    'django_migrations',
    'sqlite_sequence',
    'auth_group_permissions',
    'auth_user_groups',
    'auth_user_user_permissions',
    'django_admin_log',
    'django_content_type',
    'auth_permission',
    'auth_group',
    'auth_user',
    'django_session',
}

def sanitize_table_name(name: str) -> str:
    safe = re.sub(r'[^A-Za-z0-9_-]+', '_', name).strip('_')
    if safe and safe[0].isdigit():
        safe = '_' + safe
    return safe or 'table'


def ensure_unique_table_name(cursor, name: str) -> str:
    base = name
    i = 1
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    existing = {r[0] for r in cursor.fetchall()}
    while name in existing:
        name = f"{base}_{i}"
        i += 1
    return name


def normalize_numeric(val: str):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return val
    s = str(val).strip()
    if s == '':
        return None
    # nahrad tisícové mezery a tečky/čárky do EN formátu
    s2 = s.replace('\xa0', ' ').replace(' ', '')
    # pokud obsahuje čárku a tečku, předpokládej český formát: tisícovky tečka, desetinná čárka
    if ',' in s2 and '.' in s2:
        s2 = s2.replace('.', '').replace(',', '.')
    elif ',' in s2 and '.' not in s2:
        # pravděpodobně desetinná čárka
        s2 = s2.replace(',', '.')
    try:
        if s2.isdigit() or (s2.startswith('-') and s2[1:].isdigit()):
            return int(s2)
        return float(s2)
    except Exception:
        return s  # ponech jako text


def home(request):
    return render(request, 'home.html')

def ask(request):
    table_name = request.GET.get('table') or request.POST.get('table')
    conn = sqlite3.connect('db.sqlite3')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    # odstranit systémové tabulky
    tables = [t for t in tables if t not in EXCLUDED_TABLES]
    structure = None
    answer_columns = []
    answer_rows = []
    save_msg = ""
    if table_name:
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        structure = cursor.fetchall()
    if request.method == 'POST' and structure:
        # Rozlišení, zda jde o uložení nebo dotaz
        if 'save_answer' in request.POST:
            selected = []
            filters = []
            params = []
            summary_ops = {}
            # Sběr vstupů
            for col in structure:
                col_name = col[1]
                col_type = col[2]
                if request.POST.get(f'select_{col_name}'):
                    selected.append(col_name)
                value = request.POST.get(f'value_{col_name}', '').strip()
                operator = request.POST.get(f'operator_{col_name}', '')
                if value != '':
                    if col_type in ["INTEGER", "REAL"]:
                        if operator in ['=', '<', '>']:
                            filters.append(f'"{col_name}" {operator} ?')
                            params.append(value)
                    else:  # TEXT
                        if operator == 'exact':
                            filters.append(f'"{col_name}" = ?')
                            params.append(value)
                        elif operator == 'startswith':
                            filters.append(f'"{col_name}" LIKE ?')
                            params.append(f'{value}%')
                        elif operator == 'contains':
                            filters.append(f'"{col_name}" LIKE ?')
                            params.append(f'%{value}%')
                sop = (request.POST.get(f'summary_operator_{col_name}', '') or '').upper()
                if sop in ['SUM', 'AVERAGE', 'COUNT', 'MAX', 'MIN']:
                    # COUNT povolíme pro všechny typy, ostatní pro čísla
                    if sop == 'COUNT' or col_type in ["INTEGER", "REAL"]:
                        summary_ops[col_name] = sop
            if summary_ops:
                group_by_cols = [c for c in selected if c not in summary_ops]
                agg_exprs = []
                for c, op in summary_ops.items():
                    if op == 'SUM':
                        agg_exprs.append(f'SUM("{c}") AS "SUM_{c}"')
                    elif op == 'AVERAGE':
                        agg_exprs.append(f'AVG("{c}") AS "AVERAGE_{c}"')
                    elif op == 'COUNT':
                        agg_exprs.append(f'COUNT("{c}") AS "COUNT_{c}"')
                    elif op == 'MAX':
                        agg_exprs.append(f'MAX("{c}") AS "MAX_{c}"')
                    elif op == 'MIN':
                        agg_exprs.append(f'MIN("{c}") AS "MIN_{c}"')
                select_parts = [f'"{c}"' for c in group_by_cols] + agg_exprs
                if select_parts:
                    sql = f'SELECT {", ".join(select_parts)} FROM "{table_name}"'
                    if filters:
                        sql += ' WHERE ' + ' AND '.join(filters)
                    if group_by_cols:
                        sql += ' GROUP BY ' + ', '.join([f'"{c}"' for c in group_by_cols])
                    cursor.execute(sql, params)
                    answer_rows = cursor.fetchall()
                    # sestav seznam názvů
                    answer_columns = group_by_cols + [f'{op}_{c}' for c, op in summary_ops.items()]
            else:
                if selected:
                    fields = ', '.join([f'"{name}"' for name in selected])
                    sql = f'SELECT {fields} FROM "{table_name}"'
                    if filters:
                        sql += ' WHERE ' + ' AND '.join(filters)
                    cursor.execute(sql, params)
                    answer_rows = cursor.fetchall()
                    answer_columns = selected
            # Uložení do nové tabulky (včetně agregací)
            save_name = request.POST.get('save_name', '').strip()
            # EXPORT větve se řeší níže společně – zde jen SAVE
            if save_name and answer_columns and answer_rows:
                # map typů pro původní sloupec
                type_map = {col[1]: col[2] for col in structure}
                col_defs = []
                for ac in answer_columns:
                    if '_' in ac:
                        prefix, orig = ac.split('_', 1)
                        # prefix může být SUM/AVERAGE/COUNT/MAX/MIN
                        if prefix == 'COUNT':
                            col_defs.append(f'"{ac}" INTEGER')
                        elif prefix in ['SUM', 'AVERAGE', 'MAX', 'MIN']:
                            col_defs.append(f'"{ac}" REAL')
                        else:
                            # neznámý prefix fallback TEXT
                            col_defs.append(f'"{ac}" REAL')
                    else:
                        # group-by sloupec
                        sqltype = type_map.get(ac, 'TEXT')
                        col_defs.append(f'"{ac}" {sqltype}')
                cursor.execute(f'DROP TABLE IF EXISTS "{save_name}"')
                col_defs_sql = ', '.join(col_defs)
                cursor.execute(f'CREATE TABLE "{save_name}" ({col_defs_sql})')
                placeholders = ', '.join(['?' for _ in answer_columns])
                ins_sql = f'INSERT INTO "{save_name}" VALUES ({placeholders})'
                for r in answer_rows:
                    cursor.execute(ins_sql, r)
                conn.commit()
                save_msg = f'Tabulka "{save_name}" byla uložena.'
        else:
            # Standardní dotaz + podpora agregací SUM/AVERAGE/COUNT/MAX/MIN
            selected = []  # group-by sloupec
            filters = []
            params = []
            summary_ops = {}
            for col in structure:
                col_name = col[1]
                col_type = col[2]
                if request.POST.get(f'select_{col_name}'):
                    selected.append(col_name)
                value = request.POST.get(f'value_{col_name}', '').strip()
                operator = request.POST.get(f'operator_{col_name}', '')
                if value != '':
                    if col_type in ["INTEGER", "REAL"]:
                        if operator in ['=', '<', '>']:
                            filters.append(f'"{col_name}" {operator} ?')
                            params.append(value)
                    else:
                        if operator == 'exact':
                            filters.append(f'"{col_name}" = ?')
                            params.append(value)
                        elif operator == 'startswith':
                            filters.append(f'"{col_name}" LIKE ?')
                            params.append(f'{value}%')
                        elif operator == 'contains':
                            filters.append(f'"{col_name}" LIKE ?')
                            params.append(f'%{value}%')
                sop = (request.POST.get(f'summary_operator_{col_name}', '') or '').upper()
                if sop in ['SUM', 'AVERAGE', 'COUNT', 'MAX', 'MIN']:
                    if sop == 'COUNT' or col_type in ["INTEGER", "REAL"]:
                        summary_ops[col_name] = sop
            if summary_ops:
                group_by_cols = [c for c in selected if c not in summary_ops]
                agg_exprs = []
                for c, op in summary_ops.items():
                    if op == 'SUM':
                        agg_exprs.append(f'SUM("{c}") AS "SUM_{c}"')
                    elif op == 'AVERAGE':
                        agg_exprs.append(f'AVG("{c}") AS "AVERAGE_{c}"')
                    elif op == 'COUNT':
                        agg_exprs.append(f'COUNT("{c}") AS "COUNT_{c}"')
                    elif op == 'MAX':
                        agg_exprs.append(f'MAX("{c}") AS "MAX_{c}"')
                    elif op == 'MIN':
                        agg_exprs.append(f'MIN("{c}") AS "MIN_{c}"')
                select_parts = [f'"{c}"' for c in group_by_cols] + agg_exprs
                if select_parts:
                    sql = f'SELECT {", ".join(select_parts)} FROM "{table_name}"'
                    if filters:
                        sql += ' WHERE ' + ' AND '.join(filters)
                    if group_by_cols:
                        sql += ' GROUP BY ' + ', '.join([f'"{c}"' for c in group_by_cols])
                    cursor.execute(sql, params)
                    answer_rows = cursor.fetchall()
                    answer_columns = group_by_cols + [f'{op}_{c}' for c, op in summary_ops.items()]
            else:
                if selected:
                    fields = ', '.join([f'"{name}"' for name in selected])
                    sql = f'SELECT {fields} FROM "{table_name}"'
                    if filters:
                        sql += ' WHERE ' + ' AND '.join(filters)
                    cursor.execute(sql, params)
                    answer_rows = cursor.fetchall()
                    answer_columns = selected
        # Export CSV/JSON pokud byl požadavek
        if 'export_csv' in request.POST or 'export_json' in request.POST:
            # uživatelský název souboru (bez přípony) pokud zadán
            raw_name = (request.POST.get('save_name', '') or '').strip()
            if raw_name:
                # nahradit nepovolené znaky podtržítkem
                safe_base = re.sub(r'[^A-Za-z0-9_-]+', '_', raw_name).strip('_')
                if not safe_base:
                    safe_base = f'answer_{table_name or "result"}'
            else:
                safe_base = f'answer_{table_name or "result"}'
            if 'export_csv' in request.POST:
                output = io.StringIO()
                writer = csv.writer(output)
                if answer_columns:
                    writer.writerow(answer_columns)
                for r in (answer_rows or []):
                    writer.writerow(r)
                resp = HttpResponse(output.getvalue(), content_type='text/csv')
                fname = f'{safe_base}.csv'
                resp['Content-Disposition'] = f'attachment; filename="{fname}"'
                conn.close()
                return resp
            else:  # export_json
                rows_json = []
                for r in (answer_rows or []):
                    obj = {}
                    for i, c in enumerate(answer_columns or []):
                        obj[c] = r[i]
                    rows_json.append(obj)
                data = json.dumps({'columns': answer_columns or [], 'rows': rows_json}, ensure_ascii=False, indent=2)
                resp = HttpResponse(data, content_type='application/json; charset=utf-8')
                fname = f'{safe_base}.json'
                resp['Content-Disposition'] = f'attachment; filename="{fname}"'
                conn.close()
                return resp
    conn.close()
    return render(request, 'ask.html', {
        'tables': tables,
        'structure': structure,
        'table_name': table_name,
        'answer_columns': answer_columns,
        'answer_rows': answer_rows,
        'save_msg': save_msg
    })


def view(request):
    msg = ''
    conn = sqlite3.connect('db.sqlite3')
    cursor = conn.cursor()
    # Dvoufázové mazání: nejdříve POST s delete_table zobrazí potvrzovací stránku,
    # po potvrzení (field 'confirm' == 'yes') dojde ke smazání.
    if request.method == 'POST' and request.POST.get('delete_table'):
        tbl = request.POST.get('delete_table')
        # pokud není potvrzení, vykreslíme potvrzovací stránku
        if request.POST.get('confirm') != 'yes':
            # render confirmation template
            conn.close()
            return render(request, 'confirm_delete.html', {'table_name': tbl})
        # pokud potvrzeno, provedeme smazání
        if tbl in EXCLUDED_TABLES:
            msg = f'Cannot delete system table "{tbl}".'
        else:
            try:
                cursor.execute(f'DROP TABLE IF EXISTS "{tbl}"')
                conn.commit()
                msg = f'Table "{tbl}" deleted.'
            except Exception as e:
                msg = f'Error deleting table "{tbl}": {e}'

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    # odstranit systémové tabulky
    tables = [t for t in tables if t not in EXCLUDED_TABLES]
    conn.close()
    return render(request, 'view.html', {'tables': tables, 'msg': msg})


# Nová view pro editaci obsahu tabulky (řádků)
def edit_table(request, table_name):
    msg = ''
    if table_name in EXCLUDED_TABLES:
        msg = f'Cannot edit system table "{table_name}".'
        # zobrazíme přehled tabulek s chybou
        conn = sqlite3.connect('db.sqlite3')
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        tables = [t for t in tables if t not in EXCLUDED_TABLES]
        conn.close()
        return render(request, 'view.html', {'tables': tables, 'msg': msg})

    conn = sqlite3.connect('db.sqlite3')
    cursor = conn.cursor()

    # načítání metadat sloupců
    try:
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        cols_info = cursor.fetchall()
        # cols_info entries: (cid, name, type, notnull, dflt_value, pk)
        cols = [c[1] for c in cols_info]
        # prepare metadata list for template (name + type + index)
        cols_meta = []
        for idx, c in enumerate(cols_info):
            cols_meta.append({'idx': idx, 'name': c[1], 'type': (c[2] or '').upper()})
    except Exception as e:
        conn.close()
        return render(request, 'create_table.html', {'msg': f'Error reading table info: {e}'})

    # helper: convert string value to proper python type according to sql type
    def convert_value(raw, col_type):
        if raw is None or raw == '':
            return None, None
        t = (col_type or '').upper()
        # integer types
        if 'INT' in t and not ('CHAR' in t or 'TEXT' in t):
            try:
                return int(raw), None
            except Exception as e:
                return None, f'Cannot convert "{raw}" to INTEGER for type {col_type}'
        # real/float/num types
        if 'REAL' in t or 'FLOA' in t or 'NUM' in t or 'DEC' in t or 'DOUB' in t:
            try:
                return float(raw), None
            except Exception as e:
                return None, f'Cannot convert "{raw}" to REAL for type {col_type}'
        # default: text
        return raw, None

    # POST handling: update rows, delete row, add row
    if request.method == 'POST':
        try:
            # Delete single row (by rowid)
            if request.POST.get('delete_row'):
                rid = request.POST.get('delete_row')
                cursor.execute(f'DELETE FROM "{table_name}" WHERE rowid=?', (rid,))
                conn.commit()
                msg = f'Row {rid} deleted.'

            # Add new row
            elif request.POST.get('add_row'):
                # collect new values by index
                new_vals = []
                new_cols = []
                errors = []
                for idx, col in enumerate(cols):
                    key = f'new_{idx}'
                    raw = request.POST.get(key, '').strip()
                    conv, err = convert_value(raw, cols_meta[idx]['type'])
                    if err:
                        errors.append(f'Column {col}: {err}')
                    new_vals.append(conv)
                    new_cols.append(col)
                placeholders = ','.join(['?'] * len(new_cols))
                quoted_cols = ', '.join([f'"{n}"' for n in new_cols])
                insert_sql = f'INSERT INTO "{table_name}" ({quoted_cols}) VALUES ({placeholders})'
                cursor.execute(insert_sql, new_vals)
                conn.commit()
                msg = 'New row added.'
                if errors:
                    msg += ' Warnings: ' + '; '.join(errors)

            # Save edits for all rows
            elif request.POST.get('save'):
                # get list of rowids currently in DB to iterate
                cursor.execute(f'SELECT rowid FROM "{table_name}"')
                rowids = [r[0] for r in cursor.fetchall()]
                errors = []
                for rid in rowids:
                    # build update values by index
                    set_parts = []
                    params = []
                    for idx, col in enumerate(cols):
                        key = f'cell_{rid}_{idx}'
                        if key in request.POST:
                            raw = request.POST.get(key, '').strip()
                            conv, err = convert_value(raw, cols_meta[idx]['type'])
                            if err:
                                errors.append(f'Row {rid} Col {col}: {err}')
                            set_parts.append(f'"{col}" = ?')
                            params.append(conv)
                    if set_parts:
                        sql = f'UPDATE "{table_name}" SET {", ".join(set_parts)} WHERE rowid = ?'
                        params.append(rid)
                        cursor.execute(sql, params)
                conn.commit()
                if errors:
                    msg = 'Changes saved with warnings: ' + '; '.join(errors)
                else:
                    msg = 'Changes saved.'
        except Exception as e:
            conn.rollback()
            msg = f'Error processing POST: {e}'

    # načíst data (rowid + values)
    try:
        cursor.execute(f'SELECT rowid, * FROM "{table_name}"')
        rows_raw = cursor.fetchall()
        # cursor.description aligns with (rowid, col1, col2...)
        description = [d[0] for d in cursor.description]
        # drop the first name 'rowid' from description for columns
        # but keep rowid in rows
        # build rows as list of dict with rowid and values by column name
        rows = []
        for r in rows_raw:
            rowid = r[0]
            values = {}
            for i, col in enumerate(cols, start=1):
                values[col] = r[i]
            # create ordered list of cells so template can iterate without dict indexing
            cells = []
            # include type per cell
            col_types = {c['name']: c['type'] for c in cols_meta}
            for idx, col in enumerate(cols):
                cells.append({'idx': idx, 'col': col, 'val': values.get(col), 'type': col_types.get(col, '')})
            rows.append({'rowid': rowid, 'cells': cells})
    except Exception as e:
        conn.close()
        return render(request, 'view.html', {'tables': [], 'msg': f'Error reading table data: {e}'})

    conn.close()
    return render(request, 'edit_table.html', {'table_name': table_name, 'cols': cols, 'cols_meta': cols_meta, 'rows': rows, 'msg': msg})


def view_table(request, table_name):
    conn = sqlite3.connect('db.sqlite3')
    cursor = conn.cursor()
    try:
        cursor.execute(f'SELECT * FROM "{table_name}"')
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
    except Exception as e:
        rows = []
        columns = []
        error = str(e)
        return render(request, 'view_table.html', {'table_name': table_name, 'error': error})
    conn.close()
    return render(request, 'view_table.html', {'table_name': table_name, 'columns': columns, 'rows': rows})

def createtable(request):
    msg = ""
    # Edit mode: GET?edit=<table>
    edit_name = request.GET.get('edit') if request.method == 'GET' else None
    prefill = []  # list of (name, type)
    if edit_name:
        try:
            conn = sqlite3.connect('db.sqlite3')
            cur = conn.cursor()
            cur.execute(f'PRAGMA table_info("{edit_name}")')
            info = cur.fetchall()
            for col in info:
                # PRAGMA returns (cid, name, type, notnull, dflt_value, pk)
                prefill.append((col[1], col[2]))
            conn.close()
        except Exception as e:
            msg = f'Error loading table for edit: {e}'
    if request.method == 'POST':
        table_name = request.POST.get('table_name', '').strip()
        edit_original = request.POST.get('edit_original', '').strip()
        fields = []
        for i in range(1, 11):
            field_name = request.POST.get(f'field_name_{i}', '').strip()
            field_type = request.POST.get(f'field_type_{i}', '').strip()
            if field_name and field_type:
                fields.append((field_name, field_type))
        if table_name and fields:
            conn = sqlite3.connect('db.sqlite3')
            cursor = conn.cursor()
            try:
                # if editing existing table, drop it first (note: data will be lost)
                if edit_original:
                    cursor.execute(f'DROP TABLE IF EXISTS "{edit_original}"')
                field_defs = ', '.join([f'"{name}" {ftype}' for name, ftype in fields])
                sql = f'CREATE TABLE "{table_name}" ({field_defs});'
                cursor.execute(sql)
                conn.commit()
                msg = f'Table "{table_name}" created!'
            except Exception as e:
                msg = f'Error: {e}'
            conn.close()
        else:
            msg = 'Fill all required fields.'
    # prepare context for template: prefill up to provided range
    # build lists for 1..10 where existing prefill values are used
    prefill_rows = []
    for i in range(10):
        name = ''
        ptype = ''
        if i < len(prefill):
            name, ptype = prefill[i]
        prefill_rows.append({'name': name, 'type': ptype})
    context = {'msg': msg, 'range': range(1, 11), 'prefill_rows': prefill_rows}
    if edit_name:
        context['edit'] = True
        context['edit_name'] = edit_name
    return render(request, 'create_table.html', context)


# --- Helpery pro detekci a konverzi typů (použito v mezikroku importu) ---
from dateutil import parser as date_parser

def detect_column_type(series):
    non_empty = [v for v in series if v not in [None, '', ' ']]
    if not non_empty:
        return 'TEXT'
    def looks_int(x):
        try:
            if isinstance(x, int):
                return True
            if isinstance(x, str):
                s = x.strip()
                if s.startswith('-'):
                    s = s[1:]
                if s.isdigit():
                    int(x.strip())
                    return True
            return False
        except Exception:
            return False
    def looks_float(x):
        try:
            if isinstance(x, float):
                return True
            s = str(x).strip().replace('\xa0',' ')  # NBSP
            s = s.replace(' ', '')
            if ',' in s and '.' in s:
                s = s.replace('.', '').replace(',', '.')
            elif ',' in s and '.' not in s:
                s = s.replace(',', '.')
            float(s)
            return True
        except Exception:
            return False
    def looks_date(x):
        s = str(x).strip()
        for fmt in ['%Y-%m-%d', '%d.%m.%Y']:
            try:
                import datetime
                datetime.datetime.strptime(s, fmt)
                return True
            except Exception:
                pass
        if any(ch in s for ch in ['-', '.']):
            try:
                date_parser.parse(s, fuzzy=True)
                return True
            except Exception:
                return False
        return False
    def looks_datetime(x):
        s = str(x).strip()
        for fmt in ['%Y-%m-%d %H:%M:%S', '%d.%m.%Y %H:%M:%S']:
            try:
                import datetime
                datetime.datetime.strptime(s, fmt)
                return True
            except Exception:
                pass
        if any(ch in s for ch in ['-', '.']) and ':' in s:
            try:
                date_parser.parse(s, fuzzy=True)
                return True
            except Exception:
                return False
        return False
    sample = non_empty[:50]
    if all(looks_datetime(v) for v in sample):
        return 'DATETIME'
    if all(looks_date(v) for v in sample):
        return 'DATE'
    if all(looks_int(v) for v in sample):
        return 'INTEGER'
    if all(looks_float(v) for v in sample):
        return 'REAL'
    return 'TEXT'

def apply_conversion(value, target_type, num_format, date_format):
    if value in [None, '', ' ']:
        return None
    s = str(value).strip()
    if target_type in ['INTEGER', 'REAL']:
        if num_format == 'cz':
            s2 = s.replace('\xa0',' ').replace(' ', '')
            s2 = s2.replace('.', '')
            s2 = s2.replace(',', '.')
        elif num_format == 'us':
            s2 = s.replace(',', '')
        else:  # auto
            s2 = s.replace('\xa0',' ').replace(' ', '')
            if ',' in s2 and '.' in s2:
                s2 = s2.replace('.', '').replace(',', '.')
            elif ',' in s2 and '.' not in s2:
                s2 = s2.replace(',', '.')
        try:
            if target_type == 'INTEGER':
                return int(float(s2))
            return float(s2)
        except Exception:
            return None
    if target_type in ['DATE', 'DATETIME']:
        import datetime
        try:
            if date_format == 'iso':
                dt = datetime.datetime.strptime(s, '%Y-%m-%d')
                return dt.strftime('%Y-%m-%d')
            elif date_format == 'iso_dt':
                dt = datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            elif date_format == 'cz':
                dt = datetime.datetime.strptime(s, '%d.%m.%Y')
                return dt.strftime('%Y-%m-%d')
            elif date_format == 'cz_dt':
                dt = datetime.datetime.strptime(s, '%d.%m.%Y %H:%M:%S')
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            else:  # auto
                dt = date_parser.parse(s, fuzzy=True)
                if target_type == 'DATE':
                    return dt.strftime('%Y-%m-%d')
                return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            return None
    return s  # TEXT fallback


def imports_view(request):
    """Kompletní import view s mezikrokem preview."""
    imports_dir = os.path.join(settings.BASE_DIR, 'imports')
    os.makedirs(imports_dir, exist_ok=True)
    msg = ''
    web_url = ''
    web_tables = []
    ote_date = ''
    ote_time_resolution = 'PT15M'
    ote_url = ''
    ote_tables = []
    # --- PREVIEW akce ---
    if request.method == 'POST':
        # WEB: příprava preview
        if request.POST.get('prepare_web'):
            web_url = (request.POST.get('web_url') or '').strip()
            idx_raw = request.POST.get('import_web_index')
            suggest = (request.POST.get('import_web_table') or '').strip()
            if not web_url or idx_raw is None:
                msg = 'Chybí URL nebo index.'
            else:
                dfs = IMPORT_CACHE['web'].get(web_url)
                if dfs is None:
                    msg = 'Nejdříve načti tabulky.'
                else:
                    try:
                        idx = int(idx_raw)
                        if idx < 0 or idx >= len(dfs):
                            msg = 'Neplatný index.'
                        else:
                            df = dfs[idx]
                            cols_meta = []
                            for c in df.columns:
                                det = detect_column_type(df[c].head(200))
                                cols_meta.append({'current': str(c), 'new': sanitize_table_name(str(c)), 'detected': det, 'forced': ''})
                            preview_table = df.head(20).to_html(border=1, index=False)
                            return render(request, 'import_preview.html', {
                                'msg': 'Uprav názvy / typy před importem',
                                'source_kind': 'WEB',
                                'source_url': web_url,
                                'source_index': idx,
                                'suggest_name': sanitize_table_name(suggest or f'web_import_{idx}') ,
                                'columns': cols_meta,
                                'preview_table': preview_table
                            })
                    except Exception as e:
                        msg = f'Chyba přípravy: {e}'
        # OTE: příprava preview
        elif request.POST.get('prepare_ote'):
            ote_date = (request.POST.get('ote_date') or '').strip()
            ote_time_resolution = (request.POST.get('ote_time_resolution') or 'PT15M').strip() or 'PT15M'
            idx_raw = request.POST.get('import_ote_index')
            suggest = (request.POST.get('import_ote_table') or '').strip()
            if not ote_date or idx_raw is None:
                msg = 'Chybí datum nebo index.'
            else:
                params = {'time_resolution': ote_time_resolution, 'date': ote_date}
                ote_url = f'https://www.ote-cr.cz/cs/kratkodobe-trhy/elektrina/denni-trh?{urlencode(params)}'
                dfs = IMPORT_CACHE['web'].get(ote_url)
                if dfs is None:
                    msg = 'Nejdříve načti OTE tabulky.'
                else:
                    try:
                        idx = int(idx_raw)
                        if idx < 0 or idx >= len(dfs):
                            msg = 'Neplatný index.'
                        else:
                            df = dfs[idx]
                            cols_meta = []
                            for c in df.columns:
                                det = detect_column_type(df[c].head(200))
                                cols_meta.append({'current': str(c), 'new': sanitize_table_name(str(c)), 'detected': det, 'forced': ''})
                            preview_table = df.head(20).to_html(border=1, index=False)
                            return render(request, 'import_preview.html', {
                                'msg': 'Uprav názvy / typy před importem',
                                'source_kind': 'OTE',
                                'source_url': ote_url,
                                'source_index': idx,
                                'suggest_name': sanitize_table_name(suggest or f'ote_{ote_time_resolution.lower()}_{ote_date.replace("-", "")}_{idx}'),
                                'columns': cols_meta,
                                'preview_table': preview_table
                            })
                    except Exception as e:
                        msg = f'Chyba přípravy OTE: {e}'
        # Potvrzení preview (WEB i OTE)
        elif request.POST.get('confirm_import'):
            source_kind = request.POST.get('source_kind')
            source_url = request.POST.get('source_url')
            source_index = request.POST.get('source_index')
            final_name = sanitize_table_name(request.POST.get('final_table_name') or 'import_table')
            try:
                dfs = IMPORT_CACHE['web'].get(source_url)
                if dfs is None:
                    msg = 'Data nejsou v cache – načti znovu.'
                else:
                    idx = int(source_index)
                    if idx < 0 or idx >= len(dfs):
                        msg = 'Index mimo rozsah.'
                    else:
                        df = dfs[idx]
                        new_cols = []
                        target_types = []
                        numfmts = []
                        datefmts = []
                        for i, c in enumerate(df.columns):
                            new_name = sanitize_table_name(request.POST.get(f'rename_{i}') or str(c))
                            forced = (request.POST.get(f'force_{i}') or '').upper()
                            if not forced:
                                forced = detect_column_type(df[c].head(200))
                            new_cols.append(new_name)
                            target_types.append(forced)
                            numfmts.append(request.POST.get(f'numfmt_{i}') or 'auto')
                            datefmts.append(request.POST.get(f'datefmt_{i}') or 'auto')
                        conn = sqlite3.connect('db.sqlite3')
                        cur = conn.cursor()
                        final_name = ensure_unique_table_name(cur, final_name)
                        col_defs = []
                        for n, t in zip(new_cols, target_types):
                            sql_t = 'TEXT' if t in ['DATE','DATETIME'] else t
                            col_defs.append(f'"{n}" {sql_t}')
                        cur.execute(f'DROP TABLE IF EXISTS "{final_name}"')
                        cur.execute(f'CREATE TABLE "{final_name}" ({", ".join(col_defs)})')
                        placeholders = ','.join(['?'] * len(new_cols))
                        quoted_cols = ', '.join([f'"{n}"' for n in new_cols])
                        ins_sql = f'INSERT INTO "{final_name}" ({quoted_cols}) VALUES ({placeholders})'
                        for _, row in df.iterrows():
                            vals = []
                            for v, t, nf, dfmt in zip(row.tolist(), target_types, numfmts, datefmts):
                                vals.append(apply_conversion(v, t, nf, dfmt))
                            cur.execute(ins_sql, vals)
                        conn.commit()
                        IMPORT_LOGS.insert(0, {
                            'imported_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'source': source_url,
                            'source_type': source_kind,
                            'table_name': final_name,
                            'rows': int(df.shape[0]),
                            'cols': int(df.shape[1])
                        })
                        if len(IMPORT_LOGS) > 100:
                            IMPORT_LOGS[:] = IMPORT_LOGS[:100]
                        conn.close()
                        msg = f'Import dokončen do tabulky "{final_name}".'
            except Exception as e:
                msg = f'Chyba dokončení: {e}'
        # --- Standardní akce (načtení) ---
        elif request.POST.get('fetch_web'):
            web_url = (request.POST.get('web_url') or '').strip()
            if not web_url:
                msg = 'Zadejte URL.'
            else:
                try:
                    import requests
                    from bs4 import BeautifulSoup
                    from urllib.parse import urljoin
                    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'}
                    resp = requests.get(web_url, headers=headers, timeout=20)
                    resp.raise_for_status()
                    html = resp.text
                    try:
                        dfs = pd.read_html(html)
                    except Exception:
                        dfs = []
                    def parse_tables(source_html, base_url=None):
                        out = []
                        soup = BeautifulSoup(source_html, 'lxml')
                        raw_tables = soup.find_all('table')
                        for t in raw_tables:
                            headers_row = []
                            thead = t.find('thead')
                            if thead:
                                ths = thead.find_all('th')
                                if ths:
                                    headers_row = [th.get_text(strip=True) for th in ths]
                            if not headers_row:
                                first_tr = t.find('tr')
                                if first_tr:
                                    headers_row = [cell.get_text(strip=True) for cell in first_tr.find_all(['th','td'])]
                            data_rows = []
                            tbody = t.find('tbody')
                            rows_iter = tbody.find_all('tr') if tbody else t.find_all('tr')
                            skip_first = bool(headers_row) and (not thead)
                            for idx_r, tr in enumerate(rows_iter):
                                if skip_first and idx_r == 0:
                                    continue
                                cells = [c.get_text(strip=True) for c in tr.find_all(['td','th'])]
                                if cells:
                                    data_rows.append(cells)
                            if data_rows or headers_row:
                                max_len = max([len(r) for r in data_rows] + [len(headers_row)]) if (data_rows or headers_row) else 0
                                if max_len == 0:
                                    continue
                                if not headers_row:
                                    headers_norm = [f'col_{i}' for i in range(max_len)]
                                else:
                                    headers_norm = headers_row + [f'col_{i}' for i in range(len(headers_row), max_len)]
                                norm_rows = []
                                for r in data_rows:
                                    if len(r) < max_len:
                                        r = r + [''] * (max_len - len(r))
                                    norm_rows.append(r)
                                import pandas as _pd
                                df_m = _pd.DataFrame(norm_rows, columns=headers_norm)
                                out.append(df_m)
                        if base_url is not None:
                            for fr in soup.find_all('iframe'):
                                src = fr.get('src')
                                if src:
                                    try:
                                        u = urljoin(base_url, src)
                                        ir = requests.get(u, headers=headers, timeout=15)
                                        ir.raise_for_status()
                                        out.extend(parse_tables(ir.text))
                                    except Exception:
                                        pass
                        return out
                    if not dfs:
                        dfs = parse_tables(html, base_url=web_url)
                    IMPORT_CACHE['web'][web_url] = dfs
                    for i, df in enumerate(dfs):
                        preview_html = df.head(20).to_html(classes='web-preview', border=1, index=False)
                        web_tables.append({'preview': preview_html, 'suggest': f'web_table_{i}'})
                    if not web_tables:
                        msg = f'Na URL {web_url} nebyly nalezeny žádné tabulky.'
                except Exception as e:
                    msg = f'Chyba načítání: {e}'
        elif request.POST.get('fetch_web_js'):
            web_url = (request.POST.get('web_url') or '').strip()
            if not web_url:
                msg = 'Zadejte URL.'
            else:
                try:
                    from requests_html import HTMLSession
                    from bs4 import BeautifulSoup
                    session = HTMLSession()
                    r = session.get(web_url, headers={'User-Agent': 'Mozilla/5.0'})
                    r.html.render(timeout=30, sleep=2)
                    html = r.html.html
                    try:
                        dfs = pd.read_html(html)
                    except Exception:
                        dfs = []
                    soup = BeautifulSoup(html, 'lxml')
                    for fr in soup.find_all('iframe'):
                        src = fr.get('src')
                        if src:
                            try:
                                ir = session.get(src)
                                try:
                                    dfs_iframe = pd.read_html(ir.text)
                                except Exception:
                                    dfs_iframe = []
                                for d in dfs_iframe:
                                    dfs.append(d)
                            except Exception:
                                pass
                    IMPORT_CACHE['web'][web_url] = dfs
                    for i, df in enumerate(dfs):
                        preview_html = df.head(20).to_html(border=1, index=False)
                        web_tables.append({'preview': preview_html, 'suggest': f'web_table_{i}'})
                    if not web_tables:
                        msg = f'Na URL {web_url} nebyly nalezeny žádné tabulky (JS).'
                except Exception as e:
                    msg = f'Chyba JS načtení: {e}'
        elif request.POST.get('fetch_ote'):
            ote_date = (request.POST.get('ote_date') or '').strip()
            ote_time_resolution = (request.POST.get('ote_time_resolution') or 'PT15M').strip() or 'PT15M'
            if ote_date:
                params = {'time_resolution': ote_time_resolution, 'date': ote_date}
                ote_url = f'https://www.ote-cr.cz/cs/kratkodobe-trhy/elektrina/denni-trh?{urlencode(params)}'
                try:
                    import requests
                    from bs4 import BeautifulSoup
                    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'}
                    resp = requests.get(ote_url, headers=headers, timeout=20)
                    resp.raise_for_status()
                    html = resp.text
                    try:
                        dfs = pd.read_html(html)
                    except Exception:
                        dfs = []
                    if not dfs:
                        soup = BeautifulSoup(html, 'lxml')
                        raw_tables = soup.find_all('table')
                        dfs = []
                        for t in raw_tables:
                            headers_row = []
                            thead = t.find('thead')
                            if thead:
                                ths = thead.find_all('th')
                                if ths:
                                    headers_row = [th.get_text(strip=True) for th in ths]
                            if not headers_row:
                                first_tr = t.find('tr')
                                if first_tr:
                                    headers_row = [cell.get_text(strip=True) for cell in first_tr.find_all(['th','td'])]
                            data_rows = []
                            tbody = t.find('tbody')
                            rows_iter = tbody.find_all('tr') if tbody else t.find_all('tr')
                            skip_first = bool(headers_row) and (not thead)
                            for idx_r, tr in enumerate(rows_iter):
                                if skip_first and idx_r == 0:
                                    continue
                                cells = [c.get_text(strip=True) for c in tr.find_all(['td','th'])]
                                if cells:
                                    data_rows.append(cells)
                            if data_rows or headers_row:
                                max_len = max([len(r) for r in data_rows] + [len(headers_row)]) if (data_rows or headers_row) else 0
                                if max_len == 0:
                                    continue
                                if not headers_row:
                                    headers_norm = [f'col_{i}' for i in range(max_len)]
                                else:
                                    headers_norm = headers_row + [f'col_{i}' for i in range(len(headers_row), max_len)]
                                norm_rows = []
                                for r in data_rows:
                                    if len(r) < max_len:
                                        r = r + [''] * (max_len - len(r))
                                    norm_rows.append(r)
                                import pandas as _pd
                                df_m = _pd.DataFrame(norm_rows, columns=headers_norm)
                                dfs.append(df_m)
                    IMPORT_CACHE['web'][ote_url] = dfs
                    for i, df in enumerate(dfs):
                        df_norm = df.applymap(normalize_numeric)
                        ote_tables.append({'preview': df_norm.head(20).to_html(border=1, index=False), 'suggest': f'ote_{ote_time_resolution.lower()}_{ote_date.replace("-", "")}_{i}'})
                    if not ote_tables:
                        msg = f'Žádné OTE tabulky pro datum {ote_date}.'
                except Exception as e:
                    msg = f'Chyba načítání OTE: {e}'
        elif request.POST.get('import_web'):
            # zachováno staré okamžité importování (bez preview) pokud uživatel klikne přímo Importovat
            web_url = (request.POST.get('web_url') or '').strip()
            idx_raw = request.POST.get('import_web_index')
            table_name_req = (request.POST.get('import_web_table') or '').strip()
            if not web_url or idx_raw is None:
                msg = 'Chybí URL nebo index.'
            else:
                dfs = IMPORT_CACHE['web'].get(web_url)
                if dfs is None:
                    msg = 'Nejdříve načti tabulky.'
                else:
                    try:
                        idx = int(idx_raw)
                        if idx < 0 or idx >= len(dfs):
                            msg = 'Neplatný index.'
                        else:
                            df = dfs[idx].applymap(normalize_numeric)
                            conn = sqlite3.connect('db.sqlite3')
                            cur = conn.cursor()
                            tname = ensure_unique_table_name(cur, sanitize_table_name(table_name_req or f'web_import_{idx}'))
                            safe_cols = [sanitize_table_name(str(c)) for c in df.columns]
                            def map_dt(s):
                                has_float = s.apply(lambda x: isinstance(x, float)).any()
                                has_num = s.apply(lambda x: isinstance(x, (int, float))).any()
                                if has_float:
                                    return 'REAL'
                                if has_num:
                                    return 'INTEGER'
                                return 'TEXT'
                            col_types = [map_dt(df[c]) for c in df.columns]
                            cols_def = ', '.join([f'"{n}" {t}' for n, t in zip(safe_cols, col_types)])
                            cur.execute(f'DROP TABLE IF EXISTS "{tname}"')
                            cur.execute(f'CREATE TABLE "{tname}" ({cols_def})')
                            placeholders = ','.join(['?'] * len(safe_cols))
                            quoted_cols = ', '.join([f'"{n}"' for n in safe_cols])
                            ins_sql = f'INSERT INTO "{tname}" ({quoted_cols}) VALUES ({placeholders})'
                            for _, row in df.iterrows():
                                vals = []
                                for v, t in zip(row.tolist(), col_types):
                                    if v is None or (isinstance(v, str) and v == ''):
                                        vals.append(None)
                                    elif t == 'INTEGER':
                                        try:
                                            vals.append(int(v))
                                        except Exception:
                                            vals.append(None)
                                    elif t == 'REAL':
                                        try:
                                            vals.append(float(v))
                                        except Exception:
                                            vals.append(None)
                                    else:
                                        vals.append(str(v))
                            cur.execute(ins_sql, vals)
                            conn.commit()
                            IMPORT_LOGS.insert(0, {
                                'imported_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'source': web_url,
                                'source_type': 'WEB',
                                'table_name': tname,
                                'rows': int(df.shape[0]),
                                'cols': int(df.shape[1])
                            })
                            if len(IMPORT_LOGS) > 100:
                                IMPORT_LOGS[:] = IMPORT_LOGS[:100]
                            conn.close()
                            msg = f'Tabulka "{tname}" importována z webu.'
                    except Exception as e:
                        msg = f'Chyba importu z webu: {e}'
        elif request.POST.get('import_ote'):
            # okamžité import OTE bez preview (původní tlačítko Importovat)
            ote_date = (request.POST.get('ote_date') or '').strip()
            ote_time_resolution = (request.POST.get('ote_time_resolution') or 'PT15M').strip() or 'PT15M'
            idx_raw = request.POST.get('import_ote_index')
            table_name_req = (request.POST.get('import_ote_table') or '').strip()
            if not ote_date or idx_raw is None:
                msg = 'Chybí datum nebo index.'
            else:
                try:
                    params = {'time_resolution': ote_time_resolution, 'date': ote_date}
                    ote_url = f'https://www.ote-cr.cz/cs/kratkodobe-trhy/elektrina/denni-trh?{urlencode(params)}'
                    dfs = IMPORT_CACHE['web'].get(ote_url)
                    if dfs is None:
                        msg = 'Nejdříve načti OTE tabulky.'
                    else:
                        idx = int(idx_raw)
                        if idx < 0 or idx >= len(dfs):
                            msg = 'Neplatný index.'
                        else:
                            df = dfs[idx].applymap(normalize_numeric)
                            conn = sqlite3.connect('db.sqlite3')
                            cur = conn.cursor()
                            tname = ensure_unique_table_name(cur, sanitize_table_name(table_name_req or f'ote_{ote_time_resolution.lower()}_{ote_date.replace("-", "")}_{idx}'))
                            safe_cols = [sanitize_table_name(str(c)) for c in df.columns]
                            def map_dt(s):
                                has_float = s.apply(lambda x: isinstance(x, float)).any()
                                has_num = s.apply(lambda x: isinstance(x, (int, float))).any()
                                if has_float:
                                    return 'REAL'
                                if has_num:
                                    return 'INTEGER'
                                return 'TEXT'
                            col_types = [map_dt(df[c]) for c in df.columns]
                            cols_def = ', '.join([f'"{n}" {t}' for n, t in zip(safe_cols, col_types)])
                            cur.execute(f'DROP TABLE IF EXISTS "{tname}"')
                            cur.execute(f'CREATE TABLE "{tname}" ({cols_def})')
                            placeholders = ','.join(['?'] * len(safe_cols))
                            quoted_cols = ', '.join([f'"{n}"' for n in safe_cols])
                            ins_sql = f'INSERT INTO "{tname}" ({quoted_cols}) VALUES ({placeholders})'
                            for _, row in df.iterrows():
                                vals = []
                                for v, t in zip(row.tolist(), col_types):
                                    if v is None or (isinstance(v, str) and v == ''):
                                        vals.append(None)
                                    elif t == 'INTEGER':
                                        try:
                                            vals.append(int(v))
                                        except Exception:
                                            vals.append(None)
                                    elif t == 'REAL':
                                        try:
                                            vals.append(float(v))
                                        except Exception:
                                            vals.append(None)
                                    else:
                                        vals.append(str(v))
                            cur.execute(ins_sql, vals)
                            conn.commit()
                            IMPORT_LOGS.insert(0, {
                                'imported_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'source': ote_url,
                                'source_type': 'OTE',
                                'table_name': tname,
                                'rows': int(df.shape[0]),
                                'cols': int(df.shape[1])
                            })
                            if len(IMPORT_LOGS) > 100:
                                IMPORT_LOGS[:] = IMPORT_LOGS[:100]
                            conn.close()
                            msg = f'Tabulka "{tname}" importována (OTE).'
                except Exception as e:
                    msg = f'Chyba importu OTE: {e}'
        # Upload souboru
        elif 'file' in request.FILES:
            uploaded = request.FILES['file']
            filename = os.path.basename(uploaded.name)
            dest = os.path.join(imports_dir, filename)
            try:
                with open(dest, 'wb') as f:
                    for chunk in uploaded.chunks():
                        f.write(chunk)
                msg = f'Soubor "{filename}" nahrán.'
            except Exception as e:
                msg = f'Chyba uploadu: {e}'
        # Smazání souboru
        elif request.POST.get('delete_file'):
            name = request.POST.get('delete_file')
            path = os.path.join(imports_dir, name)
            if os.path.exists(path):
                try:
                    os.remove(path)
                    msg = f'Soubor "{name}" smazán.'
                except Exception as e:
                    msg = f'Chyba mazání: {e}'
            else:
                msg = 'Soubor nenalezen.'
        # Konverze CSV/JSON do tabulky
        elif request.POST.get('convert_file'):
            name = request.POST.get('convert_file')
            table_name = sanitize_table_name(request.POST.get('convert_table') or os.path.splitext(name)[0])
            path = os.path.join(imports_dir, name)
            if not os.path.exists(path):
                msg = 'Soubor nenalezen.'
            else:
                ext = os.path.splitext(name)[1].lower()
                try:
                    conn = sqlite3.connect('db.sqlite3')
                    cur = conn.cursor()
                    if ext == '.csv':
                        text = None
                        used_enc = None
                        for enc in ('utf-8','cp1250'):
                            try:
                                with open(path,'r',encoding=enc) as f:
                                    text = f.read()
                                used_enc = enc
                                break
                            except Exception:
                                pass
                        if text is None:
                            raise Exception('Nelze načíst CSV.')
                        rdr = csv.reader(io.StringIO(text))
                        rows = list(rdr)
                        if not rows:
                            raise Exception('CSV je prázdné.')
                        headers = [h.strip() for h in rows[0]]
                        data_rows = rows[1:]
                        def infer(values):
                            is_int = True; is_real = True
                            for v in values:
                                if v in [None,'']:
                                    continue
                                try:
                                    int(v)
                                except Exception:
                                    is_int = False
                                try:
                                    float(v)
                                except Exception:
                                    is_real = False
                            if is_int:
                                return 'INTEGER'
                            if is_real:
                                return 'REAL'
                            return 'TEXT'
                        col_types = []
                        for i,h in enumerate(headers):
                            vals = [r[i] if i < len(r) else '' for r in data_rows]
                            col_types.append(infer(vals))
                        cols_def = ', '.join([f'"{h}" {t}' for h,t in zip(headers,col_types)])
                        cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                        cur.execute(f'CREATE TABLE "{table_name}" ({cols_def})')
                        placeholders = ','.join(['?']*len(headers))
                        quoted = ', '.join([f'"{h}"' for h in headers])
                        ins = f'INSERT INTO "{table_name}" ({quoted}) VALUES ({placeholders})'
                        for r in data_rows:
                            row_vals = []
                            for j,h in enumerate(headers):
                                val = r[j] if j < len(r) else ''
                                if val == '':
                                    row_vals.append(None)
                                else:
                                    t = col_types[j]
                                    if t == 'INTEGER':
                                        try: row_vals.append(int(val))
                                        except Exception: row_vals.append(None)
                                    elif t == 'REAL':
                                        try: row_vals.append(float(val))
                                        except Exception: row_vals.append(None)
                                    else:
                                        row_vals.append(val)
                            cur.execute(ins,row_vals)
                        conn.commit()
                        conn.close()
                        msg = f'CSV převedeno do "{table_name}" (encoding {used_enc}).'
                    elif ext == '.json':
                        with open(path,'r',encoding='utf-8') as f:
                            data = json.load(f)
                        if isinstance(data, dict):
                            keys = list(data.keys())
                            rows = list(zip(*[data[k] for k in keys]))
                            headers = keys
                            data_rows = [list(r) for r in rows]
                        elif isinstance(data, list):
                            headers = []
                            for item in data:
                                if isinstance(item, dict):
                                    for k in item.keys():
                                        if k not in headers:
                                            headers.append(k)
                            data_rows = []
                            for item in data:
                                data_rows.append([item.get(h) for h in headers])
                        else:
                            raise Exception('Nepodporovaný JSON tvar.')
                        def infer_json(values):
                            is_int=True; is_real=True
                            for v in values:
                                if v is None: continue
                                if isinstance(v,bool):
                                    is_int=False; is_real=False; continue
                                if isinstance(v,int): continue
                                if isinstance(v,float):
                                    is_int=False; continue
                                try: int(v)
                                except Exception: is_int=False
                                try: float(v)
                                except Exception: is_real=False
                            if is_int: return 'INTEGER'
                            if is_real: return 'REAL'
                            return 'TEXT'
                        col_types=[]
                        for i,h in enumerate(headers):
                            vals=[r[i] if i < len(r) else None for r in data_rows]
                            col_types.append(infer_json(vals))
                        conn = sqlite3.connect('db.sqlite3'); cur = conn.cursor()
                        cols_def = ', '.join([f'"{h}" {t}' for h,t in zip(headers,col_types)])
                        cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                        cur.execute(f'CREATE TABLE "{table_name}" ({cols_def})')
                        placeholders = ','.join(['?']*len(headers))
                        quoted = ', '.join([f'"{h}"' for h in headers])
                        ins = f'INSERT INTO "{table_name}" ({quoted}) VALUES ({placeholders})'
                        for r in data_rows:
                            row_vals=[]
                            for j,h in enumerate(headers):
                                val = r[j] if j < len(r) else None
                                if val is None:
                                    row_vals.append(None)
                                else:
                                    t = col_types[j]
                                    if t == 'INTEGER':
                                        try: row_vals.append(int(val))
                                        except Exception: row_vals.append(None)
                                    elif t == 'REAL':
                                        try: row_vals.append(float(val))
                                        except Exception: row_vals.append(None)
                                    else:
                                        row_vals.append(str(val))
                            cur.execute(ins,row_vals)
                        conn.commit(); conn.close()
                        msg = f'JSON převeden do "{table_name}".'
                    else:
                        msg = 'Podporovány jen CSV/JSON.'
                except Exception as e:
                    msg = f'Chyba konverze: {e}'
                finally:
                    try: conn.close()
                    except Exception: pass
    # Seznam souborů
    files = []
    for fn in sorted(os.listdir(imports_dir)):
        p = os.path.join(imports_dir, fn)
        if os.path.isfile(p):
            st = os.stat(p)
            files.append({'name': fn, 'size_kb': round(st.st_size/1024,2), 'mtime': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(st.st_mtime))})
    return render(request, 'import.html', {
        'imported_files': files,
        'msg': msg,
        'web_tables': web_tables,
        'web_url': web_url,
        'ote_tables': ote_tables,
        'ote_url': ote_url,
        'ote_date': ote_date,
        'ote_time_resolution': ote_time_resolution,
        'import_logs': IMPORT_LOGS
    })
