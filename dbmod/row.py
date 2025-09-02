from config import DUPLICATE_KEY_CONFIG, PRIMARY_KEYS, READ_ONLY_COLUMNS, WRITE_ONLY_CONFIG
from functions import get_db_connection, get_table_schema, is_composite_pk
from flask import Blueprint, redirect, request, session, url_for

row = Blueprint('row', __name__)


@row.route('/<string:table_name>/add_row', methods=['POST'])
def add_row(table_name):
    connection = None
    if 'db_user' not in session:
        return redirect(url_for('base_routes.login'))

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
                return redirect(url_for('dbview.index', table_name=table_name, error="No valid data provided to add."))

            cols = ', '.join(f'`{key}`' for key in cleaned_data.keys())
            placeholders = ', '.join(['%s'] * len(cleaned_data))
            sql = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"

            try:
                cursor.execute(sql, list(cleaned_data.values()))
                connection.commit()

                # On successful creation, redirect to main table view
                return redirect(url_for('dbview.index', table_name=table_name))

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
                                    return redirect(url_for('dbview.expanded_view', table_name=table_name, row_id=row_id_path))
                                else:
                                    # User is already a contributor, just redirect to expanded view
                                    if isinstance(primary_key_config, list):
                                        row_id_path = '/'.join(str(existing_row[pk]) for pk in primary_key_config)
                                    else:
                                        row_id_path = str(existing_row[primary_key_config])
                                    return redirect(url_for('dbview.expanded_view', table_name=table_name, row_id=row_id_path))
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
        return redirect(url_for('dbview.index', table_name=table_name, error=str(e)))
    finally:
        if connection:
            connection.close()

    return redirect(url_for('dbview.index', table_name=table_name))


@row.route('/<string:table_name>/update_row', methods=['POST'])
def update_row(table_name):
    connection = None
    if 'db_user' not in session:
        return redirect(url_for('base_routes.login'))

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
                 return redirect(url_for('dbview.index', table_name=table_name, error="No updatable data provided."))

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
        return redirect(url_for('dbview.index', table_name=table_name, error=str(e)))
    finally:
        if connection:
            connection.close()

    return redirect(url_for('dbview.index', table_name=table_name))


@row.route('/<string:table_name>/delete_row', methods=['POST'])
def delete_row(table_name):
    connection = None
    if 'db_user' not in session:
        return redirect(url_for('base_routes.login'))

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
                            return redirect(url_for('dbview.index', table_name=table_name, error=f"Error: Missing part of composite key for deletion. Expected key: '{pk_col}'"))
                        where_clauses.append(f"`{pk_col}` = %s")
                        values.append(row_id)

                    # First, check if the row exists and get the current contributors
                    where_clause = ' AND '.join(where_clauses)
                    check_sql = f"SELECT `{contributor_column}` FROM `{table_name}` WHERE {where_clause}"
                    cursor.execute(check_sql, tuple(values))
                    row = cursor.fetchone()

                    if not row:
                        return redirect(url_for('dbview.index', table_name=table_name, error="Row not found."))

                    # Check if current user is the owner (first contributor)
                    contributors = row[contributor_column]
                    if contributors:
                        contributors_list = [c.strip() for c in contributors.split(',')]
                        if not contributors_list or contributors_list[0] != session['db_user']:
                            return redirect(url_for('dbview.index', table_name=table_name, error="Only the owner can delete this row."))
                    else:
                        return redirect(url_for('dbview.index', table_name=table_name, error="No contributors found for this row."))

                    # If we get here, user is the owner - proceed with deletion
                    sql = f"DELETE FROM `{table_name}` WHERE {where_clause}"
                    cursor.execute(sql, tuple(values))

                else:
                    # Handle single primary key
                    primary_key = primary_key_config
                    row_id = request.form.get(primary_key)
                    if not row_id:
                        return redirect(url_for('dbview.index', table_name=table_name, error=f"Error: Missing primary key for deletion. Expected key: '{primary_key}'."))

                    # First, check if the row exists and get the current contributors
                    check_sql = f"SELECT `{contributor_column}` FROM `{table_name}` WHERE `{primary_key}` = %s"
                    cursor.execute(check_sql, (row_id,))
                    row = cursor.fetchone()

                    if not row:
                        return redirect(url_for('dbview.index', table_name=table_name, error="Row not found."))

                    # Check if current user is the owner (first contributor)
                    contributors = row[contributor_column]
                    if contributors:
                        contributors_list = [c.strip() for c in contributors.split(',')]
                        if not contributors_list or contributors_list[0] != session['db_user']:
                            return redirect(url_for('dbview.index', table_name=table_name, error="Only the owner can delete this row."))
                    else:
                        return redirect(url_for('dbview.index', table_name=table_name, error="No contributors found for this row."))

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
                            return redirect(url_for('dbview.index', table_name=table_name, error=f"Error: Missing part of composite key for deletion. Expected key: '{pk_col}'"))
                        where_clauses.append(f"`{pk_col}` = %s")
                        values.append(row_id)

                    sql = f"DELETE FROM `{table_name}` WHERE {' AND '.join(where_clauses)}"
                    cursor.execute(sql, tuple(values))
                else:
                    # Handle single primary key
                    primary_key = primary_key_config
                    row_id = request.form.get(primary_key)
                    if not row_id:
                        return redirect(url_for('dbview.index', table_name=table_name, error=f"Error: Missing primary key for deletion. Expected key: '{primary_key}'."))

                    sql = f"DELETE FROM `{table_name}` WHERE `{primary_key}` = %s"
                    cursor.execute(sql, (row_id,))

        connection.commit()
    except Exception as e:
        print(f"Error deleting row: {e}")
        return redirect(url_for('dbview.index', table_name=table_name, error=str(e)))
    finally:
        if connection:
            connection.close()

    return redirect(url_for('dbview.index', table_name=table_name))