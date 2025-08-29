from config import PRIMARY_KEYS
from flask import Blueprint, request, session
from functions import get_db_connection

fk = Blueprint('fk', __name__)

@fk.route('/search_foreign_key/<string:table_name>')
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


@fk.route('/get_foreign_key_display/<string:table_name>/<string:record_id>')
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