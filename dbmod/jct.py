from config import MANY_TO_MANY_CONFIG, PRIMARY_KEYS
from flask import Blueprint, redirect, request, session, url_for
from functions import get_db_connection
jct = Blueprint('jct', __name__)

@jct.route('/<string:table_name>/add_junction_entry', methods=['POST'])
def add_junction_entry(table_name):
    connection = None
    if 'db_user' not in session:
        return redirect(url_for('base_routes.login'))

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
        return redirect(url_for('dbview.index', table_name=table_name, error="Junction configuration not found."))

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


@jct.route('/<string:table_name>/remove_junction_entry', methods=['POST'])
def remove_junction_entry(table_name):
    connection = None
    if 'db_user' not in session:
        return redirect(url_for('base_routes.login'))

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
        return redirect(url_for('dbview.index', table_name=table_name, error="Junction configuration not found."))

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


@jct.route('/<string:table_name>/update_junction_entry', methods=['POST'])
def update_junction_entry(table_name):
    connection = None
    if 'db_user' not in session:
        return redirect(url_for('base_routes.login'))

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
        return redirect(url_for('dbview.index', table_name=table_name, error="Junction configuration not found."))

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


@jct.route('/verify_junction_id/<string:table_name>/<string:main_id>/<string:junction_id>')
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