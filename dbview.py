from config import COLUMN_WIDTHS, MANY_TO_MANY_CONFIG, PRIMARY_KEYS, READ_ONLY_COLUMNS, TABLES_TO_SHOW, VISIBLE_COLUMNS, WRITE_ONLY_CONFIG
from functions import get_db_connection, get_foreign_key_display_text, get_table_schema
from flask import Blueprint, redirect, render_template, request, send_file, session, url_for
import networkx as nx
import json

dbview = Blueprint('dbview', __name__)

@dbview.route('/<string:table_name>')
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
        return redirect(url_for('base_routes.login'))

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
        return redirect(url_for('base_routes.login', error=error))
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


@dbview.route('/<string:table_name>/<path:row_id>')
def expanded_view(table_name, row_id):
    connection = None
    error = request.args.get('error')
    row_data = None
    all_junction_data = []  # Changed to support multiple junction configurations

    if 'db_user' not in session:
        return redirect(url_for('base_routes.login'))

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