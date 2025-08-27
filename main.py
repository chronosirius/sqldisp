from flask import Flask, render_template, request, redirect, url_for, session
import pymysql
import re
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, TABLES_TO_SHOW, PRIMARY_KEYS, COLUMN_WIDTHS, MANY_TO_MANY_CONFIG, WRITE_ONLY_CONFIG, READ_ONLY_COLUMNS, HIDDEN_COLUMNS, VISIBLE_COLUMNS, FOREIGN_KEY_CONFIG, DUPLICATE_KEY_CONFIG

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
        
        # Get foreign key information
        cursor.execute(f"""
            SELECT 
                COLUMN_NAME,
                REFERENCED_TABLE_NAME,
                REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = %s 
            AND REFERENCED_TABLE_NAME IS NOT NULL
        """, (table_name,))
        foreign_keys = {row['COLUMN_NAME']: {
            'table': row['REFERENCED_TABLE_NAME'], 
            'column': row['REFERENCED_COLUMN_NAME']
        } for row in cursor.fetchall()}

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
        
        # Check if this is a foreign key
        is_foreign_key = col_name in foreign_keys
        foreign_key_info = {}
        
        if is_foreign_key:
            fk_info = foreign_keys[col_name]
            # Check if we have custom FK config
            fk_config = FOREIGN_KEY_CONFIG.get(table_name, {}).get(col_name, {})
            
            foreign_key_info = {
                'is_foreign_key': True,
                'foreign_table': fk_info['table'],
                'foreign_key': fk_info['column'],
                'search_columns': fk_config.get('search_columns', ['name', 'title', 'description']),
                'display_columns': fk_config.get('display_columns', ['name', 'title', 'description'])
            }
        
        columns_info[col_name] = {
            'type': col_type,
            'is_enum': is_enum,
            'enum_values': enum_values,
            'is_primary_key': is_primary_key,
            'is_auto_increment': 'auto_increment' in col['Extra'].lower(),
            'html_input_type': html_input_type,
            'is_read_only': is_read_only,
            **foreign_key_info
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

def get_foreign_key_display_text(connection, table_name, fk_column, fk_value):
    """Helper function to get display text for a foreign key value."""
    if not fk_value:
        return None
    
    fk_config = FOREIGN_KEY_CONFIG.get(table_name, {}).get(fk_column, {})
    if not fk_config:
        return str(fk_value)
    
    try:
        with connection.cursor() as cursor:
            foreign_table = fk_config['foreign_table']
            foreign_key = fk_config['foreign_key']
            display_columns = fk_config.get('display_columns', ['name', 'title', 'description'])
            
            # Get the primary key of the foreign table
            foreign_pk = PRIMARY_KEYS.get(foreign_table, 'id')
            if isinstance(foreign_pk, list):
                foreign_pk = foreign_pk[0]  # Use first column of composite key
            
            # Build the select statement
            select_columns = [f'`{col}`' for col in display_columns if col != foreign_key]
            if foreign_key not in display_columns:
                select_columns.insert(0, f'`{foreign_key}`')
            
            columns_sql = ', '.join(select_columns)
            sql = f"SELECT {columns_sql} FROM `{foreign_table}` WHERE `{foreign_key}` = %s"
            cursor.execute(sql, (fk_value,))
            row = cursor.fetchone()
            
            if not row:
                return f"ID: {fk_value} (not found)"
            
            # Create display string
            display_parts = []
            for col in display_columns:
                if col in row and row[col] is not None:
                    display_parts.append(f"{col}: {row[col]}")
            
            if display_parts:
                return ' | '.join(display_parts[:3])  # Limit to 3 parts
            else:
                return f"ID: {fk_value}"
                
    except Exception as e:
        print(f"Error getting FK display: {e}")
        return f"ID: {fk_value}"

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
    fk_display_data = {}

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
            
            # Get foreign key display data for each row
            if data:
                primary_key_config = PRIMARY_KEYS.get(table_name)
                for row in data:
                    # Create row identifier
                    if isinstance(primary_key_config, list):
                        row_id = '/'.join(str(row[pk]) for pk in primary_key_config)
                    else:
                        row_id = str(row[primary_key_config])
                    
                    fk_display_data[row_id] = {}
                    
                    # Get display text for each foreign key column in this row
                    for col in columns_to_display:
                        if schema[col].get('is_foreign_key') and row[col] is not None:
                            display_text = get_foreign_key_display_text(connection, table_name, col, row[col])
                            fk_display_data[row_id][col] = display_text
            
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
        read_only_columns=READ_ONLY_COLUMNS.get(table_name, []),
        fk_display_data=fk_display_data
    )

@app.route('/<string:table_name>/<path:row_id>')
def expanded_view(table_name, row_id):
    connection = None
    error = request.args.get('error')
    row_data = None
    all_junction_data = []  # Changed to support multiple junction configurations
    
    if 'db_user' not in session:
        return redirect(url_for('login'))
        
    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        schema = get_table_schema(connection, table_name)
        
        with connection.cursor() as cursor:
            # 1. Fetch the main row data with proper permission checking
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
                        all_junction_data=[],
                        tables=TABLES_TO_SHOW,
                        write_only_config=WRITE_ONLY_CONFIG.get(table_name)
                    )
                
                # Build WHERE clause for composite key
                where_clauses = [f"`{col}` = %s" for col in primary_key_config]
                pk_values = pk_parts
                
                # CRITICAL: Check if the table is write-only and apply proper filtering
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
                
                # CRITICAL: Check if the table is write-only and apply proper filtering
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
            
            # CRITICAL: If no row found, this means either the row doesn't exist OR user has no permission
            if not row_data:
                if table_name in WRITE_ONLY_CONFIG:
                    error = "Access denied: You don't have permission to view this row, or it doesn't exist."
                else:
                    error = "Row not found."
                return render_template(
                    'expanded_view.html',
                    table_name=table_name,
                    row_data=None,
                    schema=schema,
                    error=error,
                    primary_key=primary_key_config,
                    all_junction_data=[],
                    tables=TABLES_TO_SHOW,
                    write_only_config=WRITE_ONLY_CONFIG.get(table_name)
                )

            # 2. Handle multiple junction table configurations
            junction_configs = MANY_TO_MANY_CONFIG.get(table_name, [])
            
            # Support both old single config format and new list format
            if isinstance(junction_configs, dict):
                junction_configs = [junction_configs]
            
            for config in junction_configs:
                junction_table = config['junction_table']
                fk_self = config['fk_self']
                fk_other = config['fk_other']
                other_table = config['other_table']
                other_display_column = config['other_display_column']
                extra_columns = config.get('extra_columns', [])
                show_multiple_rows = config.get('show_multiple_rows', False)
                junction_pk = config.get('junction_primary_key', [fk_self, fk_other])
                relationship_name = config.get('name', other_table)
                
                other_pk = PRIMARY_KEYS.get(other_table)
                if isinstance(other_pk, list):
                    other_pk = other_pk[0]  # Use first column for composite keys

                junction_data = {
                    'config': config,
                    'relationship_name': relationship_name,
                    'rows': [],
                    'all_other_options': [],
                    'junction_schema': {},
                    'other_table_schema': {}
                }

                # Get schema for junction table
                junction_data['junction_schema'] = get_table_schema(connection, junction_table)
                junction_data['other_table_schema'] = get_table_schema(connection, other_table)

                if show_multiple_rows:
                    # Fetch all junction table rows with related table data
                    junction_columns = [f"j.`{col}`" for col in [fk_self, fk_other] + extra_columns]
                    other_columns = [f"t2.`{other_pk}` as other_pk", f"t2.`{other_display_column}` as other_display"]
                    
                    # Add more columns from other table for self-references
                    if other_table == table_name:
                        # For self-references, get more detail columns
                        other_detail_columns = [col for col in junction_data['other_table_schema'].keys() 
                                              if col not in [other_pk, other_display_column]][:3]  # Limit to 3 extra columns
                        for col in other_detail_columns:
                            other_columns.append(f"t2.`{col}` as other_{col}")
                    
                    all_columns = junction_columns + other_columns
                    
                    sql_junction = (
                        f"SELECT {', '.join(all_columns)} "
                        f"FROM `{junction_table}` AS j "
                        f"JOIN `{other_table}` AS t2 ON j.{fk_other} = t2.{other_pk} "
                        f"WHERE j.{fk_self} = %s"
                    )
                    cursor.execute(sql_junction, (main_pk_value,))
                    junction_data['rows'] = cursor.fetchall()
                else:
                    # Original behavior - just show related items
                    sql_related = (
                        f"SELECT t2.{other_pk}, t2.{other_display_column} "
                        f"FROM `{junction_table}` AS j "
                        f"JOIN `{other_table}` AS t2 ON j.{fk_other} = t2.{other_pk} "
                        f"WHERE j.{fk_self} = %s"
                    )
                    cursor.execute(sql_related, (main_pk_value,))
                    junction_data['rows'] = cursor.fetchall()

                # Fetch all possible items for the dropdown (with permission filtering)
                if other_table in WRITE_ONLY_CONFIG:
                    other_contributor_column = WRITE_ONLY_CONFIG[other_table]['contributor_column']
                    sql_all_options = (
                        f"SELECT {other_pk}, {other_display_column} FROM `{other_table}` "
                        f"WHERE `{other_contributor_column}` LIKE %s"
                    )
                    cursor.execute(sql_all_options, (f"%{session['db_user']}%",))
                else:
                    sql_all_options = f"SELECT {other_pk}, {other_display_column} FROM `{other_table}`"
                    cursor.execute(sql_all_options)
                junction_data['all_other_options'] = cursor.fetchall()
                
                all_junction_data.append(junction_data)
            
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
        all_junction_data=all_junction_data,
        tables=TABLES_TO_SHOW,
        write_only_config=WRITE_ONLY_CONFIG.get(table_name),
        row_id_param=row_id
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
            
            try:
                cursor.execute(sql, list(cleaned_data.values()))
                connection.commit()
                
                # On successful creation, redirect to main table view
                return redirect(url_for('index', table_name=table_name))
                
            except Exception as insert_error:
                # Check if it's a duplicate key error and we have duplicate key config
                if "Duplicate entry" in str(insert_error) and table_name in WRITE_ONLY_CONFIG and table_name in DUPLICATE_KEY_CONFIG:
                    # Try to add the user as a contributor to the existing row using configured duplicate keys
                    try:
                        contributor_column = WRITE_ONLY_CONFIG[table_name]['contributor_column']
                        duplicate_keys = DUPLICATE_KEY_CONFIG[table_name]
                        
                        # Build search conditions using only the configured duplicate key fields
                        search_conditions = []
                        search_params = []
                        
                        for key in duplicate_keys:
                            if key in cleaned_data:
                                search_conditions.append(f"`{key}` = %s")
                                search_params.append(cleaned_data[key])
                            else:
                                # If a configured key is missing from the form data, we can't match
                                print(f"Warning: Configured duplicate key '{key}' not found in form data")
                                raise insert_error

                        if search_conditions:
                            where_clause = ' AND '.join(search_conditions)
                            find_sql = f"SELECT * FROM `{table_name}` WHERE {where_clause}"
                            cursor.execute(find_sql, tuple(search_params))
                            existing_row = cursor.fetchone()
                            
                            if existing_row:
                                # Get current contributors
                                current_contributors = existing_row.get(contributor_column, '')
                                if current_contributors:
                                    contributors_list = [c.strip() for c in current_contributors.split(',')]
                                else:
                                    contributors_list = []
                                
                                # Add current user if not already present
                                if session['db_user'] not in contributors_list:
                                    contributors_list.append(session['db_user'])
                                    new_contributors_str = ','.join(contributors_list)
                                    
                                    # Update the existing row
                                    if isinstance(primary_key_config, list):
                                        pk_conditions = []
                                        pk_params = []
                                        for pk_col in primary_key_config:
                                            pk_conditions.append(f"`{pk_col}` = %s")
                                            pk_params.append(existing_row[pk_col])
                                        pk_where_clause = ' AND '.join(pk_conditions)
                                        update_sql = f"UPDATE `{table_name}` SET `{contributor_column}` = %s WHERE {pk_where_clause}"
                                        cursor.execute(update_sql, tuple([new_contributors_str] + pk_params))
                                        row_id_path = '/'.join(str(existing_row[pk]) for pk in primary_key_config)
                                    else:
                                        update_sql = f"UPDATE `{table_name}` SET `{contributor_column}` = %s WHERE `{primary_key_config}` = %s"
                                        cursor.execute(update_sql, (new_contributors_str, existing_row[primary_key_config]))
                                        row_id_path = str(existing_row[primary_key_config])
                                    
                                    connection.commit()
                                    return redirect(url_for('expanded_view', table_name=table_name, row_id=row_id_path))
                                else:
                                    # User is already a contributor, just redirect to expanded view
                                    if isinstance(primary_key_config, list):
                                        row_id_path = '/'.join(str(existing_row[pk]) for pk in primary_key_config)
                                    else:
                                        row_id_path = str(existing_row[primary_key_config])
                                    return redirect(url_for('expanded_view', table_name=table_name, row_id=row_id_path))
                            else:
                                # No matching row found with configured keys, this shouldn't happen with a duplicate error
                                print(f"Warning: Duplicate error but no matching row found for keys: {duplicate_keys}")
                                raise insert_error
                        else:
                            # No valid search conditions could be built
                            raise insert_error
                            
                    except Exception as contributor_error:
                        print(f"Error adding as contributor: {contributor_error}")
                        # Fall through to regular error handling
                
                # If we get here, it's a different kind of error or table not configured for duplicate handling
                raise insert_error
                
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
            
            # Check if the table is write-only and enforce owner-only deletion
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
                    
                    # First, check if the row exists and get the current contributors
                    where_clause = ' AND '.join(where_clauses)
                    check_sql = f"SELECT `{contributor_column}` FROM `{table_name}` WHERE {where_clause}"
                    cursor.execute(check_sql, tuple(values))
                    row = cursor.fetchone()
                    
                    if not row:
                        return redirect(url_for('index', table_name=table_name, error="Row not found."))
                    
                    # Check if current user is the owner (first contributor)
                    contributors = row[contributor_column]
                    if contributors:
                        contributors_list = [c.strip() for c in contributors.split(',')]
                        if not contributors_list or contributors_list[0] != session['db_user']:
                            return redirect(url_for('index', table_name=table_name, error="Only the owner can delete this row."))
                    else:
                        return redirect(url_for('index', table_name=table_name, error="No contributors found for this row."))
                    
                    # If we get here, user is the owner - proceed with deletion
                    sql = f"DELETE FROM `{table_name}` WHERE {where_clause}"
                    cursor.execute(sql, tuple(values))
                    
                else:
                    # Handle single primary key
                    primary_key = primary_key_config
                    row_id = request.form.get(primary_key)
                    if not row_id:
                        return redirect(url_for('index', table_name=table_name, error=f"Error: Missing primary key for deletion. Expected key: '{primary_key}'."))
                    
                    # First, check if the row exists and get the current contributors
                    check_sql = f"SELECT `{contributor_column}` FROM `{table_name}` WHERE `{primary_key}` = %s"
                    cursor.execute(check_sql, (row_id,))
                    row = cursor.fetchone()
                    
                    if not row:
                        return redirect(url_for('index', table_name=table_name, error="Row not found."))
                    
                    # Check if current user is the owner (first contributor)
                    contributors = row[contributor_column]
                    if contributors:
                        contributors_list = [c.strip() for c in contributors.split(',')]
                        if not contributors_list or contributors_list[0] != session['db_user']:
                            return redirect(url_for('index', table_name=table_name, error="Only the owner can delete this row."))
                    else:
                        return redirect(url_for('index', table_name=table_name, error="No contributors found for this row."))
                    
                    # If we get here, user is the owner - proceed with deletion
                    sql = f"DELETE FROM `{table_name}` WHERE `{primary_key}` = %s"
                    cursor.execute(sql, (row_id,))
            else:
                # Non-write-only tables - use original logic (no ownership restrictions)
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
    
    # Find the specific junction configuration
    junction_configs = MANY_TO_MANY_CONFIG.get(table_name, [])
    if isinstance(junction_configs, dict):
        junction_configs = [junction_configs]
    
    junction_name = request.form.get('junction_name')
    config = None
    for jc in junction_configs:
        if jc.get('name', jc['other_table']) == junction_name:
            config = jc
            break
    
    if not config:
        return redirect(url_for('index', table_name=table_name, error="Junction configuration not found."))

    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        with connection.cursor() as cursor:
            main_id = request.form[config['fk_self']]
            other_id = request.form[config['fk_other']]
            
            # Collect extra column data
            extra_data = {}
            for col in config.get('extra_columns', []):
                value = request.form.get(f"extra_{col}")
                if value:
                    extra_data[col] = value
            
            # Build insert statement
            all_columns = [config['fk_self'], config['fk_other']] + list(extra_data.keys())
            all_values = [main_id, other_id] + list(extra_data.values())
            
            cols = ', '.join(f'`{col}`' for col in all_columns)
            placeholders = ', '.join(['%s'] * len(all_values))
            
            sql = f"INSERT INTO `{config['junction_table']}` ({cols}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(all_values))
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
    
    # Find the specific junction configuration
    junction_configs = MANY_TO_MANY_CONFIG.get(table_name, [])
    if isinstance(junction_configs, dict):
        junction_configs = [junction_configs]
    
    junction_name = request.form.get('junction_name')
    config = None
    for jc in junction_configs:
        if jc.get('name', jc['other_table']) == junction_name:
            config = jc
            break
    
    if not config:
        return redirect(url_for('index', table_name=table_name, error="Junction configuration not found."))

    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        with connection.cursor() as cursor:
            main_id = request.form[config['fk_self']]
            
            # Handle deletion by junction primary key if available
            junction_pk = config.get('junction_primary_key', [config['fk_self'], config['fk_other']])
            
            where_clauses = []
            where_values = []
            
            for pk_col in junction_pk:
                value = request.form.get(pk_col)
                if value:
                    where_clauses.append(f"`{pk_col}` = %s")
                    where_values.append(value)
            
            if where_clauses:
                where_clause = ' AND '.join(where_clauses)
                sql = f"DELETE FROM `{config['junction_table']}` WHERE {where_clause}"
                cursor.execute(sql, tuple(where_values))
            else:
                # Fallback to old method
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

@app.route('/<string:table_name>/update_junction_entry', methods=['POST'])
def update_junction_entry(table_name):
    connection = None
    if 'db_user' not in session:
        return redirect(url_for('login'))
    
    # Find the specific junction configuration
    junction_configs = MANY_TO_MANY_CONFIG.get(table_name, [])
    if isinstance(junction_configs, dict):
        junction_configs = [junction_configs]
    
    junction_name = request.form.get('junction_name')
    config = None
    for jc in junction_configs:
        if jc.get('name', jc['other_table']) == junction_name:
            config = jc
            break
    
    if not config:
        return redirect(url_for('index', table_name=table_name, error="Junction configuration not found."))

    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        with connection.cursor() as cursor:
            main_id = request.form[config['fk_self']]
            
            # Get the junction primary key values for WHERE clause
            junction_pk = config.get('junction_primary_key', [config['fk_self'], config['fk_other']])
            
            where_clauses = []
            where_values = []
            
            for pk_col in junction_pk:
                value = request.form.get(f"original_{pk_col}")  # Use original values for WHERE
                if value:
                    where_clauses.append(f"`{pk_col}` = %s")
                    where_values.append(value)
            
            # Collect extra column updates
            update_data = {}
            for col in config.get('extra_columns', []):
                value = request.form.get(f"extra_{col}")
                if value is not None:  # Allow empty strings
                    update_data[col] = value
            
            if update_data and where_clauses:
                set_clauses = [f"`{col}` = %s" for col in update_data.keys()]
                set_clause = ', '.join(set_clauses)
                where_clause = ' AND '.join(where_clauses)
                
                update_values = list(update_data.values()) + where_values
                
                sql = f"UPDATE `{config['junction_table']}` SET {set_clause} WHERE {where_clause}"
                cursor.execute(sql, tuple(update_values))
                
        connection.commit()
    except Exception as e:
        print(f"Error updating junction entry: {e}")
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
                
                # First, get the current contributors - REMOVE the contributor filter here
                select_sql = f"SELECT `{contributor_column}` FROM `{table_name}` WHERE {where_clause}"
                cursor.execute(select_sql, tuple(pk_params))
                current_row = cursor.fetchone()
                
                if not current_row:
                    return redirect(url_for('index', table_name=table_name, error="Row not found."))
                
                row_id_path = '/'.join(str(pk_values[col]) for col in primary_key_config)
            else:
                pk_value = request.form.get(primary_key_config)
                where_clause = f"`{primary_key_config}` = %s"
                pk_params = [pk_value]
                
                # First, get the current contributors - REMOVE the contributor filter here
                select_sql = f"SELECT `{contributor_column}` FROM `{table_name}` WHERE {where_clause}"
                cursor.execute(select_sql, tuple(pk_params))
                current_row = cursor.fetchone()
                
                if not current_row:
                    return redirect(url_for('expanded_view', table_name=table_name, row_id=pk_value, error="Row not found."))
                
                row_id_path = pk_value

            # Parse current contributors (assuming comma-separated)
            current_contributors = current_row[contributor_column]
            if current_contributors:
                contributors_list = [c.strip() for c in current_contributors.split(',')]
            else:
                contributors_list = []
            
            # Check if current user is the first contributor (owner)
            if not contributors_list or contributors_list[0] != session['db_user']:
                return redirect(url_for('expanded_view', table_name=table_name, row_id=row_id_path, error="Only the owner can add contributors."))
            
            # Add new contributor if not already present
            if new_contributor not in contributors_list:
                contributors_list.append(new_contributor)
                new_contributors_str = ','.join(contributors_list)
                
                # Update the row - REMOVE the contributor filter here too
                update_sql = f"UPDATE `{table_name}` SET `{contributor_column}` = %s WHERE {where_clause}"
                cursor.execute(update_sql, tuple([new_contributors_str] + pk_params))
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

@app.route('/<string:table_name>/remove_contributor', methods=['POST'])
def remove_contributor(table_name):
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
            contributor_to_remove = request.form.get('contributor_to_remove')
            
            if not contributor_to_remove:
                return redirect(url_for('index', table_name=table_name, error="No contributor specified for removal."))
            
            # Get the primary key value(s) from the form and construct WHERE clause
            if isinstance(primary_key_config, list):
                pk_values = {col: request.form.get(col) for col in primary_key_config}
                where_clauses = [f"`{col}` = %s" for col in primary_key_config]
                where_clause = ' AND '.join(where_clauses)
                pk_params = [pk_values[col] for col in primary_key_config]
                
                # First, get the current contributors and verify current user is the first one
                select_sql = f"SELECT `{contributor_column}` FROM `{table_name}` WHERE {where_clause}"
                cursor.execute(select_sql, tuple(pk_params))
                current_row = cursor.fetchone()
                
                if not current_row:
                    return redirect(url_for('index', table_name=table_name, error="Row not found."))
                
                row_id_path = '/'.join(str(pk_values[col]) for col in primary_key_config)
            else:
                pk_value = request.form.get(primary_key_config)
                where_clause = f"`{primary_key_config}` = %s"
                pk_params = [pk_value]
                
                # First, get the current contributors and verify current user is the first one
                select_sql = f"SELECT `{contributor_column}` FROM `{table_name}` WHERE {where_clause}"
                cursor.execute(select_sql, tuple(pk_params))
                current_row = cursor.fetchone()
                
                if not current_row:
                    return redirect(url_for('expanded_view', table_name=table_name, row_id=pk_value, error="Row not found."))
                
                row_id_path = pk_value

            # Parse current contributors (assuming comma-separated)
            current_contributors = current_row[contributor_column]
            if not current_contributors:
                return redirect(url_for('expanded_view', table_name=table_name, row_id=row_id_path, error="No contributors found."))
                
            contributors_list = [c.strip() for c in current_contributors.split(',')]
            
            # Check if current user is the first contributor (owner)
            if not contributors_list or contributors_list[0] != session['db_user']:
                return redirect(url_for('expanded_view', table_name=table_name, row_id=row_id_path, error="Only the owner can remove contributors."))
            
            # Check if trying to remove the first contributor (owner)
            if contributor_to_remove == contributors_list[0]:
                return redirect(url_for('expanded_view', table_name=table_name, row_id=row_id_path, error="The owner cannot be removed. Transfer ownership to someone else first if needed."))
            
            # Remove the contributor if they exist
            if contributor_to_remove in contributors_list:
                contributors_list.remove(contributor_to_remove)
                new_contributors_str = ','.join(contributors_list)
                
                # Update the row
                update_sql = f"UPDATE `{table_name}` SET `{contributor_column}` = %s WHERE {where_clause}"
                cursor.execute(update_sql, tuple([new_contributors_str] + pk_params))
                connection.commit()
            else:
                return redirect(url_for('expanded_view', table_name=table_name, row_id=row_id_path, error="Contributor not found in the list."))
        
    except Exception as e:
        print(f"Error removing contributor: {e}")
        return redirect(url_for('expanded_view', table_name=table_name, row_id=row_id_path, error=str(e)))
    finally:
        if connection:
            connection.close()

    return redirect(url_for('expanded_view', table_name=table_name, row_id=row_id_path))

@app.route('/')
def root_redirect():
    if not DEFAULT_TABLE:
        return "No tables are configured to be shown."
    return redirect(url_for('index', table_name=DEFAULT_TABLE))


@app.route('/verify_junction_id/<string:table_name>/<string:main_id>/<string:junction_id>')
def verify_junction_id(table_name, main_id, junction_id):
    """Verifies if a junction ID exists and returns information about it."""
    from flask import jsonify
    
    connection = None
    if 'db_user' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
        
    config = MANY_TO_MANY_CONFIG.get(table_name)
    if not config:
        return jsonify({'error': 'No many-to-many config found'}), 400

    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        with connection.cursor() as cursor:
            other_table = config['other_table']
            other_display_column = config['other_display_column']
            junction_table = config['junction_table']
            fk_self = config['fk_self']
            fk_other = config['fk_other']
            other_pk = PRIMARY_KEYS.get(other_table, 'id')
            
            # Handle composite primary keys for the other table
            if isinstance(other_pk, list):
                # For composite keys, we'd need to handle this differently
                # For now, assume single primary key for the other table
                other_pk = other_pk[0]
            
            # Check if the ID exists in the other table
            check_sql = f"SELECT `{other_pk}`, `{other_display_column}` FROM `{other_table}` WHERE `{other_pk}` = %s"
            cursor.execute(check_sql, (junction_id,))
            other_record = cursor.fetchone()
            
            if not other_record:
                return jsonify({
                    'exists': False,
                    'already_linked': False,
                    'display_name': None
                })
            
            # Check if this relationship already exists
            junction_check_sql = f"SELECT COUNT(*) as count FROM `{junction_table}` WHERE `{fk_self}` = %s AND `{fk_other}` = %s"
            cursor.execute(junction_check_sql, (main_id, junction_id))
            junction_result = cursor.fetchone()
            
            already_linked = junction_result['count'] > 0
            
            return jsonify({
                'exists': True,
                'already_linked': already_linked,
                'display_name': other_record[other_display_column]
            })
            
    except Exception as e:
        print(f"Error verifying junction ID: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if connection:
            connection.close()


@app.route('/search_foreign_key/<string:table_name>')
def search_foreign_key(table_name):
    """Search for foreign key options based on query."""
    from flask import jsonify
    
    connection = None
    if 'db_user' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    query = request.args.get('q', '').strip()
    search_columns_param = request.args.get('columns', '')
    
    if not query:
        return jsonify({'results': []})
    
    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        with connection.cursor() as cursor:
            search_columns = [col.strip() for col in search_columns_param.split(',') if col.strip()]
            
            if not search_columns:
                return jsonify({'results': []})
            
            # Get the primary key of the foreign table
            foreign_pk = PRIMARY_KEYS.get(table_name, 'id')
            if isinstance(foreign_pk, list):
                foreign_pk = foreign_pk[0]  # Use first column of composite key
            
            # Build search conditions
            search_conditions = []
            search_params = []
            
            for col in search_columns:
                search_conditions.append(f"`{col}` LIKE %s")
                search_params.append(f"%{query}%")
            
            # Get all columns for display
            all_columns = search_columns.copy()
            if foreign_pk not in all_columns:
                all_columns.insert(0, foreign_pk)
            
            columns_sql = ', '.join([f'`{col}`' for col in all_columns])
            where_clause = ' OR '.join(search_conditions)
            
            sql = f"SELECT {columns_sql} FROM `{table_name}` WHERE {where_clause} LIMIT 10"
            cursor.execute(sql, tuple(search_params))
            results = cursor.fetchall()
            
            # Format results
            formatted_results = []
            for row in results:
                # Create display string
                display_parts = []
                for col in search_columns:
                    if row.get(col):
                        display_parts.append(f"{col}: {row[col]}")
                
                formatted_results.append({
                    'id': row[foreign_pk],
                    'display': ' | '.join(display_parts) if display_parts else f"ID: {row[foreign_pk]}"
                })
            
            return jsonify({'results': formatted_results})
            
    except Exception as e:
        print(f"Error searching foreign key: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if connection:
            connection.close()


@app.route('/get_foreign_key_display/<string:table_name>/<string:record_id>')
def get_foreign_key_display(table_name, record_id):
    """Get display information for a specific foreign key record."""
    from flask import jsonify
    
    connection = None
    if 'db_user' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        with connection.cursor() as cursor:
            # Get the primary key of the foreign table
            foreign_pk = PRIMARY_KEYS.get(table_name, 'id')
            if isinstance(foreign_pk, list):
                foreign_pk = foreign_pk[0]  # Use first column of composite key
            
            # Get all columns to build display
            cursor.execute(f"SELECT * FROM `{table_name}` WHERE `{foreign_pk}` = %s", (record_id,))
            row = cursor.fetchone()
            
            if not row:
                return jsonify({'success': False, 'error': 'Record not found'})
            
            # Create a simple display string with key information
            display_parts = []
            for key, value in row.items():
                if key != foreign_pk and value is not None:
                    display_parts.append(f"{key}: {value}")
            
            display = ' | '.join(display_parts[:3]) if display_parts else f"Record ID: {record_id}"
            
            return jsonify({
                'success': True,
                'display': display
            })
            
    except Exception as e:
        print(f"Error getting foreign key display: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if connection:
            connection.close()


# Update the get_table_schema function to detect foreign keys:


@app.route('/favicon.ico')
def favicon():
    return '', 204

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')