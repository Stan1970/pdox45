from django.shortcuts import render
import sqlite3
import os
from django.conf import settings
import time
import json
import csv
import io

def home(request):
    return render(request, 'home.html')

def ask(request):
    table_name = request.GET.get('table')
    conn = sqlite3.connect('db.sqlite3')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
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
            for col in structure:
                col_name = col[1]
                if request.POST.get(f'select_{col_name}'):
                    selected.append(col_name)
                    value = request.POST.get(f'value_{col_name}', '').strip()
                    operator = request.POST.get(f'operator_{col_name}', '')
                    if value != '':
                        if col[2] in ["INTEGER", "REAL"]:
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
            if selected:
                fields = ', '.join([f'"{name}"' for name in selected])
                sql = f'SELECT {fields} FROM "{table_name}"'
                if filters:
                    sql += ' WHERE ' + ' AND '.join(filters)
                cursor.execute(sql, params)
                answer_rows = cursor.fetchall()
                answer_columns = selected
                # Uložení do nové tabulky
                save_name = request.POST.get('save_name', '').strip()
                if save_name and answer_columns and answer_rows:
                    col_types = {col[1]: col[2] for col in structure if col[1] in answer_columns}
                    field_defs = ', '.join([f'"{col}" {col_types[col]}' for col in answer_columns])
                    cursor.execute(f'DROP TABLE IF EXISTS "{save_name}"')
                    cursor.execute(f'CREATE TABLE "{save_name}" ({field_defs})')
                    for row in answer_rows:
                        placeholders = ', '.join(['?' for _ in row])
                        cursor.execute(f'INSERT INTO "{save_name}" VALUES ({placeholders})', row)
                    conn.commit()
                    save_msg = f'Tabulka "{save_name}" byla uložena.'
        else:
            # Standardní dotaz s operátory včetně SUM
            selected = []
            filters = []
            params = []
            sum_column = None
            for col in structure:
                col_name = col[1]
                if request.POST.get(f'select_{col_name}'):
                    operator = request.POST.get(f'operator_{col_name}', '')
                    if operator == 'sum' and col[2] in ["INTEGER", "REAL"]:
                        sum_column = col_name
                    else:
                        selected.append(col_name)
            if sum_column:
                sql = f'SELECT SUM("{sum_column}") FROM "{table_name}"'
                # Filtry pro ostatní sloupce
                for col in structure:
                    col_name = col[1]
                    if request.POST.get(f'select_{col_name}') and col_name != sum_column:
                        value = request.POST.get(f'value_{col_name}', '').strip()
                        operator = request.POST.get(f'operator_{col_name}', '')
                        if value != '':
                            if col[2] in ["INTEGER", "REAL"]:
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
                if filters:
                    sql += ' WHERE ' + ' AND '.join(filters)
                cursor.execute(sql, params)
                answer_rows = cursor.fetchall()
                answer_columns = [f'SUM({sum_column})']
            else:
                # Běžné operátory
                for col in structure:
                    col_name = col[1]
                    if request.POST.get(f'select_{col_name}'):
                        value = request.POST.get(f'value_{col_name}', '').strip()
                        operator = request.POST.get(f'operator_{col_name}', '')
                        if value != '':
                            if col[2] in ["INTEGER", "REAL"]:
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
                if selected:
                    fields = ', '.join([f'"{name}"' for name in selected])
                    sql = f'SELECT {fields} FROM "{table_name}"'
                    if filters:
                        sql += ' WHERE ' + ' AND '.join(filters)
                    cursor.execute(sql, params)
                    answer_rows = cursor.fetchall()
                    answer_columns = selected
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
    conn = sqlite3.connect('db.sqlite3')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return render(request, 'view.html', {'tables': tables})

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
    if request.method == 'POST':
        table_name = request.POST.get('table_name', '').strip()
        fields = []
        for i in range(1, 11):
            field_name = request.POST.get(f'field_name_{i}', '').strip()
            field_type = request.POST.get(f'field_type_{i}', '').strip()
            if field_name and field_type:
                fields.append((field_name, field_type))
        if table_name and fields:
            conn = sqlite3.connect('db.sqlite3')
            cursor = conn.cursor()
            field_defs = ', '.join([f'"{name}" {ftype}' for name, ftype in fields])
            sql = f'CREATE TABLE "{table_name}" ({field_defs});'
            try:
                cursor.execute(sql)
                conn.commit()
                msg = f'Table "{table_name}" created!'
            except Exception as e:
                msg = f'Error: {e}'
            conn.close()
        else:
            msg = 'Fill all required fields.'
    return render(request, 'create_table.html', {'msg': msg, 'range': range(1, 11)})


def imports_view(request):
    """Zpracovává stránku importů: výpis souborů v adresáři imports/, upload, delete a konverzi CSV/JSON do sqlite tabulky.
    Zachovává pouze CSV a JSON (bez Excelu). CSV se pokusí načíst v utf-8, pak v cp1250 jako fallback.
    """
    imports_dir = os.path.join(settings.BASE_DIR, 'imports')
    os.makedirs(imports_dir, exist_ok=True)
    msg = ''

    if request.method == 'POST':
        # upload
        if 'file' in request.FILES:
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

    return render(request, 'import.html', {'imported_files': files, 'msg': msg})
