from django.shortcuts import render
import sqlite3
import os
from django.conf import settings
import time
import json
import csv
import io
from django.http import HttpResponse
import re
import pandas as pd

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


def imports_view(request):
    """Zpracovává stránku importů: výpis souborů v adresáři imports/, upload, delete a konverzi CSV/JSON do sqlite tabulky.
    Zachovává pouze CSV a JSON (bez Excelu). CSV se pokusí načíst v utf-8, pak v cp1250 jako fallback.
    """
    imports_dir = os.path.join(settings.BASE_DIR, 'imports')
    os.makedirs(imports_dir, exist_ok=True)
    msg = ''

    web_url = ''
    web_tables = []  # list dicts: {preview: html, suggest: name}

    if request.method == 'POST':
        # Import z webu - načtení seznamu tabulek
        if request.POST.get('fetch_web'):
            web_url = (request.POST.get('web_url') or '').strip()
            if not web_url:
                msg = 'Zadejte URL.'
            else:
                try:
                    try:
                        import requests
                        from bs4 import BeautifulSoup
                        from urllib.parse import urljoin
                    except ImportError as ie:
                        msg = f'Chybějící knihovna: {ie}. Nainstalujte prosím balíčky: pip install beautifulsoup4 lxml requests html5lib'
                        web_tables = []
                        raise RuntimeError('Missing bs4')
                    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116 Safari/537.36'}
                    resp = requests.get(web_url, headers=headers, timeout=20)
                    resp.raise_for_status()
                    html = resp.text
                    # Pokus o pandas.read_html (rychlé)
                    dfs = []
                    try:
                        dfs = pd.read_html(html)
                    except Exception:
                        dfs = []
                    def parse_tables_from_html(source_html, base_url=None):
                        out = []
                        soup = BeautifulSoup(source_html, 'lxml')
                        raw_tables = soup.find_all('table')
                        for t in raw_tables:
                            # hlavička
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
                            # řádky
                            data_rows = []
                            tbody = t.find('tbody')
                            rows_iter = tbody.find_all('tr') if tbody else t.find_all('tr')
                            # pokud jsme použili první <tr> na hlavičku, přeskoč ho u dat
                            skip_first = bool(headers_row) and (not thead)
                            for idx_r, tr in enumerate(rows_iter):
                                if skip_first and idx_r == 0:
                                    continue
                                cells = [c.get_text(strip=True) for c in tr.find_all(['td','th'])]
                                if cells:
                                    data_rows.append(cells)
                            # vytvoření DF i bez hlavičky
                            if data_rows or headers_row:
                                max_len = 0
                                for r in data_rows:
                                    if len(r) > max_len:
                                        max_len = len(r)
                                if headers_row:
                                    max_len = max(max_len, len(headers_row))
                                if max_len == 0:
                                    continue
                                if not headers_row:
                                    headers_norm = [f'col_{i}' for i in range(max_len)]
                                else:
                                    headers_norm = headers_row + [f'col_{i}' for i in range(len(headers_row), max_len)]
                                normalized = []
                                for r in data_rows:
                                    if len(r) < max_len:
                                        r = r + [''] * (max_len - len(r))
                                    normalized.append(r)
                                import pandas as _pd
                                df_manual = _pd.DataFrame(normalized, columns=headers_norm)
                                out.append(df_manual)
                        # prohledej iframy
                        if base_url is not None:
                            for fr in soup.find_all('iframe'):
                                src = fr.get('src')
                                if src:
                                    try:
                                        url = urljoin(base_url, src)
                                        ir = requests.get(url, headers=headers, timeout=20)
                                        ir.raise_for_status()
                                        out.extend(parse_tables_from_html(ir.text))
                                    except Exception:
                                        pass
                        return out
                    if not dfs:
                        dfs = parse_tables_from_html(html, base_url=web_url)
                    web_tables = []
                    for i, df in enumerate(dfs):
                        preview_html = df.head(20).to_html(classes='web-preview', border=1, index=False)
                        suggest = 'web_table_{}'.format(i)
                        web_tables.append({'preview': preview_html, 'suggest': suggest})
                    if not web_tables and 'Missing bs4' not in str(ie if 'ie' in locals() else ''):
                        msg = f'Na URL {web_url} nebyly nalezeny žádné tabulky.'
                except RuntimeError:
                    pass
                except Exception as e:
                    msg = f'Chyba při načítání tabulek z URL: {e}'
        # Import z webu - načtení seznamu tabulek s JS renderováním
        elif request.POST.get('fetch_web_js'):
            web_url = (request.POST.get('web_url') or '').strip()
            if not web_url:
                msg = 'Zadejte URL.'
            else:
                try:
                    try:
                        from requests_html import HTMLSession
                        from bs4 import BeautifulSoup
                    except ImportError as ie:
                        msg = f'Chybějící knihovna pro JS render: {ie}. Nainstalujte: pip install requests-html beautifulsoup4 lxml'
                        web_tables = []
                        raise RuntimeError('Missing requests-html/bs4')
                    session = HTMLSession()
                    r = session.get(web_url, headers={'User-Agent': 'Mozilla/5.0'})
                    r.html.render(timeout=30, sleep=2)
                    html = r.html.html
                    dfs = []
                    try:
                        dfs = pd.read_html(html)
                    except Exception:
                        dfs = []
                    soup = BeautifulSoup(html, 'lxml')
                    iframes = soup.find_all('iframe')
                    for fr in iframes:
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
                    web_tables = []
                    for i, df in enumerate(dfs):
                        preview_html = df.head(20).to_html(classes='web-preview', border=1, index=False)
                        suggest = 'web_table_{}'.format(i)
                        web_tables.append({'preview': preview_html, 'suggest': suggest})
                    if not web_tables and 'Missing requests-html/bs4' not in str(ie if 'ie' in locals() else ''):
                        msg = f'Na URL {web_url} nebyly nalezeny žádné tabulky (ani po JS renderu).'
                except RuntimeError:
                    pass
                except Exception as e:
                    msg = f'Chyba při načítání tabulek (JS): {e}'
        # Import z webu - ulož vybranou tabulku do SQLite
        elif request.POST.get('import_web'):
            web_url = (request.POST.get('web_url') or '').strip()
            idx_raw = request.POST.get('import_web_index')
            table_name = (request.POST.get('import_web_table') or '').strip()
            if not web_url:
                msg = 'URL chybí.'
            elif idx_raw is None:
                msg = 'Chybí index tabulky.'
            elif not table_name:
                msg = 'Zadejte název cílové tabulky.'
            else:
                try:
                    idx = int(idx_raw)
                    dfs = pd.read_html(web_url)
                    if idx < 0 or idx >= len(dfs):
                        msg = 'Neplatný index tabulky.'
                    else:
                        df = dfs[idx]
                        conn = sqlite3.connect('db.sqlite3')
                        cur = conn.cursor()
                        # převeď názvy sloupců na bezpečné
                        safe_cols = []
                        for c in df.columns:
                            cn = re.sub(r'\W+', '_', str(c)).strip('_') or 'col'
                            safe_cols.append(cn)
                        # vytvoř tabulku: typy inferuj z pandas dtypes
                        def map_dtype(s):
                            t = str(s)
                            if 'int' in t:
                                return 'INTEGER'
                            if 'float' in t or 'double' in t:
                                return 'REAL'
                            return 'TEXT'
                        col_types = [map_dtype(df[c].dtype) for c in df.columns]
                        cols_def = ', '.join([f'"{n}" {t}' for n, t in zip(safe_cols, col_types)])
                        cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                        cur.execute(f'CREATE TABLE "{table_name}" ({cols_def})')
                        placeholders = ','.join(['?'] * len(safe_cols))
                        quoted_cols = ', '.join([f'"{n}"' for n in safe_cols])
                        insert_sql = f'INSERT INTO "{table_name}" ({quoted_cols}) VALUES ({placeholders})'
                        # vlož data
                        for _, row in df.iterrows():
                            vals = []
                            for v, t in zip(row.tolist(), col_types):
                                if pd.isna(v):
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
                            cur.execute(insert_sql, vals)
                        conn.commit()
                        conn.close()
                        msg = f'Tabulka "{table_name}" importována z webu.'
                except Exception as e:
                    msg = f'Chyba importu z webu: {e}'
        # upload
        elif 'file' in request.FILES:
            uploaded = request.FILES['file']
            filename = os.path.basename(uploaded.name)
            dest = os.path.join(imports_dir, filename)
            try:
                with open(dest, 'wb') as f:
                    for chunk in uploaded.chunks():
                        f.write(chunk)
                msg = f'File "{filename}" uploaded.'
            except Exception as e:
                msg = f'Upload error: {e}'
        # delete
        elif request.POST.get('delete_file'):
            name = request.POST.get('delete_file')
            path = os.path.join(imports_dir, name)
            if os.path.exists(path):
                try:
                    os.remove(path)
                    msg = f'File "{name}" deleted.'
                except Exception as e:
                    msg = f'Delete error: {e}'
            else:
                msg = f'File "{name}" not found.'
        # convert
        elif request.POST.get('convert_file'):
            name = request.POST.get('convert_file')
            table_name = request.POST.get('convert_table', '').strip()
            if not table_name:
                table_name = os.path.splitext(name)[0]
            path = os.path.join(imports_dir, name)
            if not os.path.exists(path):
                msg = f'File "{name}" not found.'
            else:
                ext = os.path.splitext(name)[1].lower()
                try:
                    conn = sqlite3.connect('db.sqlite3')
                    cur = conn.cursor()

                    if ext == '.csv':
                        # Try reading CSV with utf-8, fallback to cp1250
                        text = None
                        for enc in ('utf-8', 'cp1250'):
                            try:
                                with open(path, 'r', encoding=enc) as f:
                                    text = f.read()
                                used_enc = enc
                                break
                            except Exception:
                                text = None
                        if text is None:
                            raise Exception('Cannot read CSV (utf-8/cp1250).')
                        reader = csv.reader(io.StringIO(text))
                        rows = list(reader)
                        if not rows:
                            raise Exception('CSV file is empty.')
                        headers = [h.strip() for h in rows[0]]
                        data_rows = rows[1:]

                        # infer column types
                        def infer_type(values):
                            is_int = True
                            is_real = True
                            for v in values:
                                if v is None or v == '':
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
                        for ci, h in enumerate(headers):
                            vals = [r[ci] if ci < len(r) else '' for r in data_rows]
                            col_types.append(infer_type(vals))

                        # create table
                        cols_def = ', '.join([f'"{h}" {t}' for h, t in zip(headers, col_types)])
                        cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                        cur.execute(f'CREATE TABLE "{table_name}" ({cols_def})')

                        # prepare insert
                        placeholders = ','.join(['?'] * len(headers))
                        quoted_headers = [f'"{h}"' for h in headers]
                        insert_sql = 'INSERT INTO "{}" ({}) VALUES ({})'.format(table_name, ', '.join(quoted_headers), placeholders)

                        # insert rows, convert types where possible
                        for r in data_rows:
                            row = []
                            for i, h in enumerate(headers):
                                val = r[i] if i < len(r) else ''
                                if val == '':
                                    row.append(None)
                                else:
                                    ctype = col_types[i]
                                    if ctype == 'INTEGER':
                                        try:
                                            row.append(int(val))
                                        except Exception:
                                            row.append(None)
                                    elif ctype == 'REAL':
                                        try:
                                            row.append(float(val))
                                        except Exception:
                                            row.append(None)
                                    else:
                                        row.append(val)
                            cur.execute(insert_sql, row)
                        conn.commit()
                        msg = f'CSV converted into table "{table_name}" (encoding {used_enc}).'

                    elif ext == '.json':
                        with open(path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        # support list of objects or dict-of-lists
                        if isinstance(data, dict):
                            # try to convert dict of lists to rows
                            keys = list(data.keys())
                            rows = list(zip(*[data[k] for k in keys]))
                            headers = keys
                            data_rows = [list(r) for r in rows]
                        elif isinstance(data, list):
                            # list of objects
                            headers = []
                            for item in data:
                                if isinstance(item, dict):
                                    for k in item.keys():
                                        if k not in headers:
                                            headers.append(k)
                            data_rows = []
                            for item in data:
                                row = [item.get(h) for h in headers]
                                data_rows.append(row)
                        else:
                            raise Exception('Unsupported JSON structure')

                        # infer types
                        def infer_type_json(values):
                            is_int = True
                            is_real = True
                            for v in values:
                                if v is None:
                                    continue
                                if isinstance(v, bool):
                                    is_int = False
                                    is_real = False
                                    continue
                                if isinstance(v, (int,)):
                                    continue
                                if isinstance(v, (float,)):
                                    is_int = False
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
                        for ci, h in enumerate(headers):
                            vals = [r[ci] if ci < len(r) else None for r in data_rows]
                            col_types.append(infer_type_json(vals))

                        cols_def = ', '.join([f'"{h}" {t}' for h, t in zip(headers, col_types)])
                        cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                        cur.execute(f'CREATE TABLE "{table_name}" ({cols_def})')

                        placeholders = ','.join(['?'] * len(headers))
                        quoted_headers = [f'"{h}"' for h in headers]
                        insert_sql = 'INSERT INTO "{}" ({}) VALUES ({})'.format(table_name, ', '.join(quoted_headers), placeholders)

                        for r in data_rows:
                            row = []
                            for i, h in enumerate(headers):
                                val = r[i] if i < len(r) else None
                                if val is None:
                                    row.append(None)
                                else:
                                    ctype = col_types[i]
                                    if ctype == 'INTEGER':
                                        try:
                                            row.append(int(val))
                                        except Exception:
                                            row.append(None)
                                    elif ctype == 'REAL':
                                        try:
                                            row.append(float(val))
                                        except Exception:
                                            row.append(None)
                                    else:
                                        row.append(str(val))
                            cur.execute(insert_sql, row)
                        conn.commit()
                        msg = f'JSON converted into table "{table_name}".'
                    else:
                        msg = 'Unsupported file type for conversion. Only CSV and JSON supported.'
                except Exception as e:
                    msg = f'Convert error: {e}'
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass

    # prepare list of imported files
    files = []
    for fn in sorted(os.listdir(imports_dir)):
        p = os.path.join(imports_dir, fn)
        if os.path.isfile(p):
            st = os.stat(p)
            files.append({
                'name': fn,
                'size_kb': round(st.st_size/1024, 2),
                'mtime': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(st.st_mtime))
            })

    return render(request, 'import.html', {'imported_files': files, 'msg': msg, 'web_tables': web_tables, 'web_url': web_url})
