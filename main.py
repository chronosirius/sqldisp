from flask import Flask, render_template, request, redirect, url_for, session
import pymysql
import re
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, TABLES_TO_SHOW, PRIMARY_KEYS, COLUMN_WIDTHS, MANY_TO_MANY_CONFIG, WRITE_ONLY_CONFIG, READ_ONLY_COLUMNS, HIDDEN_COLUMNS, VISIBLE_COLUMNS

app = Flask(__name__)
app.secret_key = 'your_super_secret_key'

DEFAULT_TABLE = TABLES_TO_SHOW[0] if TABLES_TO_SHOW else None

def get_db_connection(user, password):
    """Establishes and returns a connection to the MySQL database."""
    return pymysql.connect(
        host=DB_HOST,
        user=user,
        password=password,
        database=DB_NAME,
        port=DB_PORT,
        cursorclass=pymysql.cursors.DictCursor
    )

def get_table_schema(connection, table_name):
    """Retrieves column information for the specified table, including ENUM and data type."""
    with connection.cursor() as cursor:
        cursor.execute(f"DESCRIBE `{table_name}`")
        schema = cursor.fetchall()

    hidden_cols = HIDDEN_COLUMNS.get(table_name, [])
    read_only_cols = READ_ONLY_COLUMNS.get(table_name, [])
    
    columns_info = {}
    for col in schema:
        col_name = col['Field']
        
        # Check if the column is explicitly hidden
        if col_name in hidden_cols:
            continue
        
        col_type = col['Type']
        is_enum = col_type.startswith('enum')
        
        enum_values = []
        if is_enum:
            match = re.search(r"enum\((.*?)\)", col_type)
            if match:
                enum_values = [v.strip("'") for v in match.group(1).split(',')]
        
        html_input_type = 'text'
        if 'int' in col_type or 'float' in col_type or 'decimal' in col_type or 'double' in col_type:
            html_input_type = 'number'
        elif 'date' in col_type:
            html_input_type = 'date'
        elif 'time' in col_type:
            html_input_type = 'time'
        elif 'email' in col_name.lower():
            html_input_type = 'email'
        
        is_primary_key = col['Key'] == 'PRI'
        is_read_only = is_primary_key or col_name in read_only_cols
        
        columns_info[col_name] = {
            'type': col_type,
            'is_enum': is_enum,
            'enum_values': enum_values,
            'is_primary_key': is_primary_key,
            'is_auto_increment': 'auto_increment' in col['Extra'].lower(),
            'html_input_type': html_input_type,
            'is_read_only': is_read_only
        }
    return columns_info


def is_composite_pk(table_name):
    """Checks if a table has a composite primary key."""
    pk_config = PRIMARY_KEYS.get(table_name)
    return isinstance(pk_config, list)

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Displays the login form on GET and handles login on POST."""
    if request.method == 'POST':
        user = request.form['user']
        password = request.form['password']
        
        try:
            get_db_connection(user, password).close()
            session['db_user'] = user
            session['db_password'] = password
            return redirect(url_for('root_redirect'))
        except Exception as e:
            error = f"Login failed: {e}"
            return render_template('login.html', error=error)
            
    error = request.args.get('error')
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    """Logs the user out by clearing the session."""
    session.clear()
    return redirect(url_for('login'))

@app.route('/<string:table_name>')
def index(table_name):
    """Displays the main database table view."""
    connection = None
    data = []
    columns_to_display = []
    schema = {}
    error = None

    if table_name not in TABLES_TO_SHOW:
        error = f"Error: Table '{table_name}' is not configured to be shown."
        return render_template('index.html', error=error, tables=TABLES_TO_SHOW)

    if 'db_user' not in session:
        return redirect(url_for('login'))
        
    error = request.args.get('error')
    
    try:
        connection = get_db_connection(
            session['db_user'], 
            session['db_password']
        )
        
        schema = get_table_schema(connection, table_name)
        
        with connection.cursor() as cursor:
            visible_cols_config = VISIBLE_COLUMNS.get(table_name)
            
            # Use configured visible columns or all columns if not specified
            if visible_cols_config:
                columns_to_display = [col for col in visible_cols_config if col in schema]
                cols_sql = ', '.join([f'`{col}`' for col in columns_to_display])
            else:
                columns_to_display = [col for col in schema.keys()]
                cols_sql = '*'

            # Check if the table is write-only
            is_write_only = table_name in WRITE_ONLY_CONFIG
            
            sql = f"SELECT {cols_sql} FROM `{table_name}`"
            
            # If write-only, filter rows by contributor
            if is_write_only:
                contributor_column = WRITE_ONLY_CONFIG[table_name]['contributor_column']
                sql += f" WHERE `{contributor_column}` LIKE %s"
                cursor.execute(sql, (f"%{session['db_user']}%",))
            else:
                cursor.execute(sql)

            data = cursor.fetchall()
            
    except Exception as e:
        error = f"Error connecting to or querying the database: {e}"
        print(error)
        session.clear()
        return redirect(url_for('login', error=error))
    finally:
        if connection:
            connection.close()
    
    return render_template(
        'index.html', 
        data=data, 
        columns=columns_to_display, 
        table_name=table_name, 
        primary_key=PRIMARY_KEYS.get(table_name), 
        error=error,
        schema=schema,
        tables=TABLES_TO_SHOW,
        column_widths=COLUMN_WIDTHS.get(table_name, []),
        many_to_many_config=MANY_TO_MANY_CONFIG.get(table_name),
        write_only_config=WRITE_ONLY_CONFIG.get(table_name),
        read_only_columns=READ_ONLY_COLUMNS.get(table_name, [])
    )

@app.route('/<string:table_name>/<path:row_id>')
def expanded_view(table_name, row_id):
    connection = None
    error = request.args.get('error')
    row_data = None
    related_data = []
    all_other_options = []
    
    if 'db_user' not in session:
        return redirect(url_for('login'))
        
    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        schema = get_table_schema(connection, table_name)
        
        with connection.cursor() as cursor:
            # 1. Fetch the main row data
            primary_key_config = PRIMARY_KEYS.get(table_name)
            
            # Handle composite vs single primary key
            if isinstance(primary_key_config, list):
                # Composite primary key - parse the row_id parameter
                pk_parts = row_id.split('/')
                if len(pk_parts) != len(primary_key_config):
                    error = f"Invalid composite primary key format. Expected {len(primary_key_config)} parts, got {len(pk_parts)}"
                    return render_template(
                        'expanded_view.html',
                        table_name=table_name,
                        row_data=None,
                        schema=schema,
                        error=error,
                        primary_key=primary_key_config,
                        many_to_many_config=None,
                        related_data=[],
                        all_other_options=[],
                        tables=TABLES_TO_SHOW,
                        write_only_config=WRITE_ONLY_CONFIG.get(table_name)
                    )
                
                # Build WHERE clause for composite key
                where_clauses = [f"`{col}` = %s" for col in primary_key_config]
                pk_values = pk_parts
                
                # Check if the table is write-only and apply filtering
                is_write_only = table_name in WRITE_ONLY_CONFIG
                if is_write_only:
                    contributor_column = WRITE_ONLY_CONFIG[table_name]['contributor_column']
                    where_clauses.append(f"`{contributor_column}` LIKE %s")
                    pk_values.append(f"%{session['db_user']}%")
                
                where_clause = ' AND '.join(where_clauses)
                sql = f"SELECT * FROM `{table_name}` WHERE {where_clause}"
                cursor.execute(sql, tuple(pk_values))
                
                # For junction table operations, we need the first primary key value
                main_pk_value = pk_parts[0]
                
            else:
                # Single primary key
                primary_key = primary_key_config
                
                # Check if the table is write-only and apply filtering
                is_write_only = table_name in WRITE_ONLY_CONFIG
                
                if is_write_only:
                    contributor_column = WRITE_ONLY_CONFIG[table_name]['contributor_column']
                    sql = f"SELECT * FROM `{table_name}` WHERE `{primary_key}` = %s AND `{contributor_column}` LIKE %s"
                    cursor.execute(sql, (row_id, f"%{session['db_user']}%"))
                else:
                    sql = f"SELECT * FROM `{table_name}` WHERE `{primary_key}` = %s"
                    cursor.execute(sql, (row_id,))
                
                main_pk_value = row_id
            
            row_data = cursor.fetchone()
            
            # If no row found, return error
            if not row_data:
                if table_name in WRITE_ONLY_CONFIG:
                    error = "Row not found or you don't have permission to view it."
                else:
                    error = "Row not found."
                return render_template(
                    'expanded_view.html',
                    table_name=table_name,
                    row_data=None,
                    schema=schema,
                    error=error,
                    primary_key=primary_key_config,
                    many_to_many_config=None,
                    related_data=[],
                    all_other_options=[],
                    tables=TABLES_TO_SHOW,
                    write_only_config=WRITE_ONLY_CONFIG.get(table_name)
                )

            # 2. Check for many-to-many relationship and fetch related data
            config = MANY_TO_MANY_CONFIG.get(table_name)
            if config:
                junction_table = config['junction_table']
                fk_self = config['fk_self']
                fk_other = config['fk_other']
                other_table = config['other_table']
                other_display_column = config['other_display_column']
                other_pk = PRIMARY_KEYS.get(other_table)

                # Fetch all related items using the main primary key value
                sql_related = (
                    f"SELECT t2.{other_pk}, t2.{other_display_column} "
                    f"FROM `{junction_table}` AS t1 "
                    f"JOIN `{other_table}` AS t2 ON t1.{fk_other} = t2.{other_pk} "
                    f"WHERE t1.{fk_self} = %s"
                )
                cursor.execute(sql_related, (main_pk_value,))
                related_data = cursor.fetchall()

                # Fetch all possible items to populate the dropdown
                sql_all_options = f"SELECT {other_pk}, {other_display_column} FROM `{other_table}`"
                cursor.execute(sql_all_options)
                all_other_options = cursor.fetchall()
            
    except Exception as e:
        error = f"Error: {e}"
        print(error)
        
    finally:
        if connection:
            connection.close()

    return render_template(
        'expanded_view.html',
        table_name=table_name,
        row_data=row_data,
        schema=schema,
        error=error,
        primary_key=PRIMARY_KEYS.get(table_name),
        many_to_many_config=config,
        related_data=related_data,
        all_other_options=all_other_options,
        tables=TABLES_TO_SHOW,
        write_only_config=WRITE_ONLY_CONFIG.get(table_name),
        row_id_param=row_id  # Pass the original row_id parameter for URL generation
    )

@app.route('/<string:table_name>/add_row', methods=['POST'])
def add_row(table_name):
    connection = None
    if 'db_user' not in session:
        return redirect(url_for('login'))

    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        with connection.cursor() as cursor:
            data = request.form.to_dict()
            schema = get_table_schema(connection, table_name)
            primary_key_config = PRIMARY_KEYS.get(table_name)
            
            cleaned_data = {}
            for key, value in data.items():
                is_auto_inc = schema.get(key, {}).get('is_auto_increment', False)
                
                if value != '':
                    cleaned_data[key] = value

            # Add contributor username if the table is write-only
            if table_name in WRITE_ONLY_CONFIG:
                contributor_column = WRITE_ONLY_CONFIG[table_name]['contributor_column']
                cleaned_data[contributor_column] = session['db_user']

            if isinstance(primary_key_config, list):
                # Composite key: The primary key fields are often not auto-incrementing
                # and are included in the form, so we don't need to do anything.
                pass
            else:
                # Single key: Remove auto-incrementing primary key if it exists
                primary_key = primary_key_config
                if primary_key and schema.get(primary_key, {}).get('is_auto_increment'):
                    if primary_key in cleaned_data:
                        del cleaned_data[primary_key]
                        
            if not cleaned_data:
                return redirect(url_for('index', table_name=table_name, error="No valid data provided to add."))

            cols = ', '.join(f'`{key}`' for key in cleaned_data.keys())
            placeholders = ', '.join(['%s'] * len(cleaned_data))
            sql = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"
            cursor.execute(sql, list(cleaned_data.values()))
        connection.commit()
    except Exception as e:
        print(f"Error adding row: {e}")
        return redirect(url_for('index', table_name=table_name, error=str(e)))
    finally:
        if connection:
            connection.close()
    
    return redirect(url_for('index', table_name=table_name))

@app.route('/<string:table_name>/update_row', methods=['POST'])
def update_row(table_name):
    connection = None
    if 'db_user' not in session:
        return redirect(url_for('login'))

    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        with connection.cursor() as cursor:
            data = request.form.to_dict()
            schema = get_table_schema(connection, table_name)
            primary_key_config = PRIMARY_KEYS.get(table_name)
            
            read_only_cols = READ_ONLY_COLUMNS.get(table_name, [])
            
            # Separate the primary key(s) from the updatable data
            updatable_data = {}
            pk_values = {}

            if isinstance(primary_key_config, list):
                # Composite key
                for key, value in data.items():
                    if key in primary_key_config:
                        pk_values[key] = value
                    elif key not in read_only_cols:
                        updatable_data[key] = value if value != '' else None
            else:
                # Single key
                primary_key = primary_key_config
                pk_values[primary_key] = data.pop(primary_key)
                for key, value in data.items():
                    if key not in read_only_cols:
                        updatable_data[key] = value if value != '' else None
            
            if not updatable_data:
                 return redirect(url_for('index', table_name=table_name, error="No updatable data provided."))

            set_clause = ', '.join(f'`{key}` = %s' for key in updatable_data.keys())
            
            # Add write-only filtering if applicable
            if table_name in WRITE_ONLY_CONFIG:
                contributor_column = WRITE_ONLY_CONFIG[table_name]['contributor_column']
                
                if isinstance(primary_key_config, list):
                    where_clauses = [f"`{col}` = %s" for col in primary_key_config]
                    where_clauses.append(f"`{contributor_column}` LIKE %s")
                    where_clause = ' AND '.join(where_clauses)
                    sql = f"UPDATE `{table_name}` SET {set_clause} WHERE {where_clause}"
                    values = list(updatable_data.values()) + [pk_values[col] for col in primary_key_config] + [f"%{session['db_user']}%"]
                else:
                    where_clause = f"`{primary_key_config}` = %s AND `{contributor_column}` LIKE %s"
                    sql = f"UPDATE `{table_name}` SET {set_clause} WHERE {where_clause}"
                    values = list(updatable_data.values()) + [pk_values[primary_key_config], f"%{session['db_user']}%"]
            else:
                if isinstance(primary_key_config, list):
                    where_clauses = [f"`{col}` = %s" for col in primary_key_config]
                    where_clause = ' AND '.join(where_clauses)
                    sql = f"UPDATE `{table_name}` SET {set_clause} WHERE {where_clause}"
                    values = list(updatable_data.values()) + [pk_values[col] for col in primary_key_config]
                else:
                    where_clause = f"`{primary_key_config}` = %s"
                    sql = f"UPDATE `{table_name}` SET {set_clause} WHERE {where_clause}"
                    values = list(updatable_data.values())
                    values.append(pk_values[primary_key_config])

            cursor.execute(sql, values)
        connection.commit()
    except Exception as e:
        print(f"Error updating row: {e}")
        return redirect(url_for('index', table_name=table_name, error=str(e)))
    finally:
        if connection:
            connection.close()
    
    return redirect(url_for('index', table_name=table_name))

@app.route('/<string:table_name>/delete_row', methods=['POST'])
def delete_row(table_name):
    connection = None
    if 'db_user' not in session:
        return redirect(url_for('login'))

    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        with connection.cursor() as cursor:
            primary_key_config = PRIMARY_KEYS.get(table_name)
            
            # Add write-only filtering if applicable
            if table_name in WRITE_ONLY_CONFIG:
                contributor_column = WRITE_ONLY_CONFIG[table_name]['contributor_column']
                
                if is_composite_pk(table_name):
                    # Handle composite primary key
                    where_clauses = []
                    values = []
                    for pk_col in primary_key_config:
                        row_id = request.form.get(pk_col)
                        if not row_id:
                            return redirect(url_for('index', table_name=table_name, error=f"Error: Missing part of composite key for deletion. Expected key: '{pk_col}'"))
                        where_clauses.append(f"`{pk_col}` = %s")
                        values.append(row_id)
                    
                    where_clauses.append(f"`{contributor_column}` LIKE %s")
                    values.append(f"%{session['db_user']}%")
                    
                    sql = f"DELETE FROM `{table_name}` WHERE {' AND '.join(where_clauses)}"
                    cursor.execute(sql, tuple(values))
                else:
                    # Handle single primary key
                    primary_key = primary_key_config
                    row_id = request.form.get(primary_key)
                    if not row_id:
                        return redirect(url_for('index', table_name=table_name, error=f"Error: Missing primary key for deletion. Expected key: '{primary_key}'."))
                    
                    sql = f"DELETE FROM `{table_name}` WHERE `{primary_key}` = %s AND `{contributor_column}` LIKE %s"
                    cursor.execute(sql, (row_id, f"%{session['db_user']}%"))
            else:
                if is_composite_pk(table_name):
                    # Handle composite primary key
                    where_clauses = []
                    values = []
                    for pk_col in primary_key_config:
                        row_id = request.form.get(pk_col)
                        if not row_id:
                            return redirect(url_for('index', table_name=table_name, error=f"Error: Missing part of composite key for deletion. Expected key: '{pk_col}'"))
                        where_clauses.append(f"`{pk_col}` = %s")
                        values.append(row_id)
                    
                    sql = f"DELETE FROM `{table_name}` WHERE {' AND '.join(where_clauses)}"
                    cursor.execute(sql, tuple(values))
                else:
                    # Handle single primary key
                    primary_key = primary_key_config
                    row_id = request.form.get(primary_key)
                    if not row_id:
                        return redirect(url_for('index', table_name=table_name, error=f"Error: Missing primary key for deletion. Expected key: '{primary_key}'."))
                    
                    sql = f"DELETE FROM `{table_name}` WHERE `{primary_key}` = %s"
                    cursor.execute(sql, (row_id,))
                
        connection.commit()
    except Exception as e:
        print(f"Error deleting row: {e}")
        return redirect(url_for('index', table_name=table_name, error=str(e)))
    finally:
        if connection:
            connection.close()
    
    return redirect(url_for('index', table_name=table_name))

@app.route('/<string:table_name>/add_junction_entry', methods=['POST'])
def add_junction_entry(table_name):
    connection = None
    if 'db_user' not in session:
        return redirect(url_for('login'))
    
    config = MANY_TO_MANY_CONFIG.get(table_name)
    if not config:
        return redirect(url_for('index', table_name=table_name, error="No many-to-many config found."))

    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        with connection.cursor() as cursor:
            main_id = request.form[config['fk_self']]
            other_id = request.form[config['fk_other']]
            
            sql = f"INSERT INTO `{config['junction_table']}` (`{config['fk_self']}`, `{config['fk_other']}`) VALUES (%s, %s)"
            cursor.execute(sql, (main_id, other_id))
        connection.commit()
    except Exception as e:
        print(f"Error adding junction entry: {e}")
        return redirect(url_for('expanded_view', table_name=table_name, row_id=main_id, error=str(e)))
    finally:
        if connection:
            connection.close()
    
    return redirect(url_for('expanded_view', table_name=table_name, row_id=main_id))


@app.route('/<string:table_name>/remove_junction_entry', methods=['POST'])
def remove_junction_entry(table_name):
    connection = None
    if 'db_user' not in session:
        return redirect(url_for('login'))
    
    config = MANY_TO_MANY_CONFIG.get(table_name)
    if not config:
        return redirect(url_for('index', table_name=table_name, error="No many-to-many config found."))

    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        with connection.cursor() as cursor:
            main_id = request.form[config['fk_self']]
            other_id = request.form[config['fk_other']]

            sql = f"DELETE FROM `{config['junction_table']}` WHERE `{config['fk_self']}` = %s AND `{config['fk_other']}` = %s"
            cursor.execute(sql, (main_id, other_id))
        connection.commit()
    except Exception as e:
        print(f"Error removing junction entry: {e}")
        return redirect(url_for('expanded_view', table_name=table_name, row_id=main_id, error=str(e)))
    finally:
        if connection:
            connection.close()
    
    return redirect(url_for('expanded_view', table_name=table_name, row_id=main_id))


@app.route('/<string:table_name>/add_contributor', methods=['POST'])
def add_contributor(table_name):
    connection = None
    if 'db_user' not in session:
        return redirect(url_for('login'))
        
    # Check if the table is configured for write-only mode and has a contributor column
    if table_name not in WRITE_ONLY_CONFIG:
        return redirect(url_for('index', table_name=table_name, error="This feature is not enabled for this table."))

    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        with connection.cursor() as cursor:
            primary_key_config = PRIMARY_KEYS.get(table_name)
            contributor_column = WRITE_ONLY_CONFIG[table_name]['contributor_column']
            new_contributor = request.form.get('new_contributor')
            
            if not new_contributor:
                return redirect(url_for('index', table_name=table_name, error="No contributor username provided."))
            
            # Get the primary key value(s) from the form and construct WHERE clause
            if isinstance(primary_key_config, list):
                pk_values = {col: request.form.get(col) for col in primary_key_config}
                where_clauses = [f"`{col}` = %s" for col in primary_key_config]
                where_clause = ' AND '.join(where_clauses)
                pk_params = [pk_values[col] for col in primary_key_config]
                
                # First, get the current contributors
                select_sql = f"SELECT `{contributor_column}` FROM `{table_name}` WHERE {where_clause} AND `{contributor_column}` = %s"
                cursor.execute(select_sql, tuple(pk_params + [session['db_user']]))
                current_row = cursor.fetchone()
                
                if not current_row:
                    return redirect(url_for('index', table_name=table_name, error="Row not found or you don't have permission to modify it."))
                
                row_id_path = '/'.join(str(pk_values[col]) for col in primary_key_config)
            else:
                pk_value = request.form.get(primary_key_config)
                where_clause = f"`{primary_key_config}` = %s"
                pk_params = [pk_value]
                
                # First, get the current contributors
                select_sql = f"SELECT `{contributor_column}` FROM `{table_name}` WHERE {where_clause} AND `{contributor_column}` = %s"
                cursor.execute(select_sql, tuple(pk_params + [session['db_user']]))
                current_row = cursor.fetchone()
                
                if not current_row:
                    return redirect(url_for('expanded_view', table_name=table_name, row_id=pk_value, error="Row not found or you don't have permission to modify it."))
                
                row_id_path = pk_value

            # Parse current contributors (assuming comma-separated)
            current_contributors = current_row[contributor_column]
            if current_contributors:
                contributors_list = [c.strip() for c in current_contributors.split(',')]
            else:
                contributors_list = []
            
            # Add new contributor if not already present
            if new_contributor not in contributors_list:
                contributors_list.append(new_contributor)
                new_contributors_str = ','.join(contributors_list)
                
                # Update the row
                update_sql = f"UPDATE `{table_name}` SET `{contributor_column}` = %s WHERE {where_clause} AND `{contributor_column}` = %s"
                cursor.execute(update_sql, tuple([new_contributors_str] + pk_params + [current_contributors]))
                connection.commit()
            else:
                return redirect(url_for('expanded_view', table_name=table_name, row_id=row_id_path, error="Contributor already has access to this row."))
        
    except Exception as e:
        print(f"Error adding contributor: {e}")
        return redirect(url_for('expanded_view', table_name=table_name, row_id=row_id_path, error=str(e)))
    finally:
        if connection:
            connection.close()

    return redirect(url_for('expanded_view', table_name=table_name, row_id=row_id_path))


@app.route('/<string:table_name>/share_row', methods=['POST'])
def share_row(table_name):
    connection = None
    if 'db_user' not in session:
        return redirect(url_for('login'))
        
    # Check if the table is configured for write-only mode and has a contributor column
    if table_name not in WRITE_ONLY_CONFIG:
        return redirect(url_for('index', table_name=table_name, error="This feature is not enabled for this table."))

    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        with connection.cursor() as cursor:
            primary_key_config = PRIMARY_KEYS.get(table_name)
            
            # Get the primary key value(s) from the form
            if isinstance(primary_key_config, list):
                pk_values = {col: request.form.get(col) for col in primary_key_config}
                where_clauses = [f"`{col}` = %s" for col in primary_key_config]
                where_clause = ' AND '.join(where_clauses)
                sql_params = [request.form.get('shared_with')] + [pk_values[col] for col in primary_key_config]
            else:
                pk_value = request.form.get(primary_key_config)
                where_clause = f"`{primary_key_config}` = %s"
                sql_params = [request.form.get('shared_with'), pk_value]

            contributor_column = WRITE_ONLY_CONFIG[table_name]['contributor_column']
            
            sql = f"UPDATE `{table_name}` SET `{contributor_column}` = %s WHERE {where_clause}"
            cursor.execute(sql, tuple(sql_params))
        
        connection.commit()
    except Exception as e:
        print(f"Error sharing row: {e}")
        return redirect(url_for('index', table_name=table_name, error=str(e)))
    finally:
        if connection:
            connection.close()

    if isinstance(primary_key_config, list):
        # For composite keys, construct the URL path
        pk_parts = [str(pk_values[col]) for col in primary_key_config]
        row_id_path = '/'.join(pk_parts)
        return redirect(url_for('expanded_view', table_name=table_name, row_id=row_id_path))
    else:
        return redirect(url_for('expanded_view', table_name=table_name, row_id=pk_value))

@app.route('/')
def root_redirect():
    if not DEFAULT_TABLE:
        return "No tables are configured to be shown."
    return redirect(url_for('index', table_name=DEFAULT_TABLE))

@app.route('/favicon.ico')
def favicon():
    return '', 204

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')