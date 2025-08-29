import pymysql
from config import *
import re

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