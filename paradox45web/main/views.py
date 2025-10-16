from django.shortcuts import render
import sqlite3

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
            # Získání vybraných sloupců z hidden polí
            selected = []
            filters = []
            params = []
            for col in structure:
                col_name = col[1]
                if request.POST.get(f'select_{col_name}'):
                    selected.append(col_name)
                    value = request.POST.get(f'value_{col_name}', '').strip()
                    if value != '':
                        filters.append(f'"{col_name}" = ?')
                        params.append(value)
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
            # Standardní dotaz
            selected = []
            filters = []
            params = []
            for col in structure:
                col_name = col[1]
                if request.POST.get(f'select_{col_name}'):
                    selected.append(col_name)
                    value = request.POST.get(f'value_{col_name}', '').strip()
                    if value != '':
                        filters.append(f'"{col_name}" = ?')
                        params.append(value)
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
    # Seznam všech tabulek v SQLite
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
        for i in range(1, 11):  # max 10 polí
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