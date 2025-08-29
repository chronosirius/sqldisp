from config import PRIMARY_KEYS, WRITE_ONLY_CONFIG
from flask import Blueprint, redirect, request, session, url_for
from functions import get_db_connection

contrib = Blueprint('contrib', __name__)

@contrib.route('/<string:table_name>/add_contributor', methods=['POST'])
def add_contributor(table_name):
    connection = None
    if 'db_user' not in session:
        return redirect(url_for('base_routes.login'))

    # Check if the table is configured for write-only mode and has a contributor column
    if table_name not in WRITE_ONLY_CONFIG:
        return redirect(url_for('dbview.index', table_name=table_name, error="This feature is not enabled for this table."))

    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        with connection.cursor() as cursor:
            primary_key_config = PRIMARY_KEYS.get(table_name)
            contributor_column = WRITE_ONLY_CONFIG[table_name]['contributor_column']
            new_contributor = request.form.get('new_contributor')

            if not new_contributor:
                return redirect(url_for('dbview.index', table_name=table_name, error="No contributor username provided."))

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
                    return redirect(url_for('dbview.index', table_name=table_name, error="Row not found."))

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
                    return redirect(url_for('dbview.expanded_view', table_name=table_name, row_id=pk_value, error="Row not found."))

                row_id_path = pk_value

            # Parse current contributors (assuming comma-separated)
            current_contributors = current_row[contributor_column]
            if current_contributors:
                contributors_list = [c.strip() for c in current_contributors.split(',')]
            else:
                contributors_list = []

            # Check if current user is the first contributor (owner)
            if not contributors_list or contributors_list[0] != session['db_user']:
                return redirect(url_for('dbview.expanded_view', table_name=table_name, row_id=row_id_path, error="Only the owner can add contributors."))

            # Add new contributor if not already present
            if new_contributor not in contributors_list:
                contributors_list.append(new_contributor)
                new_contributors_str = ','.join(contributors_list)

                # Update the row - REMOVE the contributor filter here too
                update_sql = f"UPDATE `{table_name}` SET `{contributor_column}` = %s WHERE {where_clause}"
                cursor.execute(update_sql, tuple([new_contributors_str] + pk_params))
                connection.commit()
            else:
                return redirect(url_for('dbview.expanded_view', table_name=table_name, row_id=row_id_path, error="Contributor already has access to this row."))

    except Exception as e:
        print(f"Error adding contributor: {e}")
        return redirect(url_for('dbview.expanded_view', table_name=table_name, row_id=row_id_path, error=str(e)))
    finally:
        if connection:
            connection.close()

    return redirect(url_for('dbview.expanded_view', table_name=table_name, row_id=row_id_path))


@contrib.route('/<string:table_name>/remove_contributor', methods=['POST'])
def remove_contributor(table_name):
    connection = None
    if 'db_user' not in session:
        return redirect(url_for('base_routes.login'))

    # Check if the table is configured for write-only mode and has a contributor column
    if table_name not in WRITE_ONLY_CONFIG:
        return redirect(url_for('dbview.index', table_name=table_name, error="This feature is not enabled for this table."))

    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
        with connection.cursor() as cursor:
            primary_key_config = PRIMARY_KEYS.get(table_name)
            contributor_column = WRITE_ONLY_CONFIG[table_name]['contributor_column']
            contributor_to_remove = request.form.get('contributor_to_remove')

            if not contributor_to_remove:
                return redirect(url_for('dbview.index', table_name=table_name, error="No contributor specified for removal."))

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
                    return redirect(url_for('dbview.index', table_name=table_name, error="Row not found."))

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
                    return redirect(url_for('dbview.expanded_view', table_name=table_name, row_id=pk_value, error="Row not found."))

                row_id_path = pk_value

            # Parse current contributors (assuming comma-separated)
            current_contributors = current_row[contributor_column]
            if not current_contributors:
                return redirect(url_for('dbview.expanded_view', table_name=table_name, row_id=row_id_path, error="No contributors found."))

            contributors_list = [c.strip() for c in current_contributors.split(',')]

            # Check if current user is the first contributor (owner)
            if not contributors_list or contributors_list[0] != session['db_user']:
                return redirect(url_for('dbview.expanded_view', table_name=table_name, row_id=row_id_path, error="Only the owner can remove contributors."))

            # Check if trying to remove the first contributor (owner)
            if contributor_to_remove == contributors_list[0]:
                return redirect(url_for('dbview.expanded_view', table_name=table_name, row_id=row_id_path, error="The owner cannot be removed. Transfer ownership to someone else first if needed."))

            # Remove the contributor if they exist
            if contributor_to_remove in contributors_list:
                contributors_list.remove(contributor_to_remove)
                new_contributors_str = ','.join(contributors_list)

                # Update the row
                update_sql = f"UPDATE `{table_name}` SET `{contributor_column}` = %s WHERE {where_clause}"
                cursor.execute(update_sql, tuple([new_contributors_str] + pk_params))
                connection.commit()
            else:
                return redirect(url_for('dbview.expanded_view', table_name=table_name, row_id=row_id_path, error="Contributor not found in the list."))

    except Exception as e:
        print(f"Error removing contributor: {e}")
        return redirect(url_for('dbview.expanded_view', table_name=table_name, row_id=row_id_path, error=str(e)))
    finally:
        if connection:
            connection.close()

    return redirect(url_for('dbview.expanded_view', table_name=table_name, row_id=row_id_path))