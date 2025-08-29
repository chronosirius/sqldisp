
# --- FIXED DATABASE CONNECTION DETAILS ---
DB_HOST = 'localhost'
DB_USER = 'your_username' # You can set a default user if you want
DB_PASSWORD = 'your_password' # You can set a default password if you want
DB_NAME = 'db_name_here'
DB_PORT = 3306

# --- DYNAMIC TABLE CONFIGURATION ---
# IMPORTANT: Specify which tables you want to show and their primary keys here.
TABLES_TO_SHOW = ['users', 'groups', 'databank'] # Add your table names here
PRIMARY_KEYS = {
    'users': 'id',
    'groups': ['id', 'creation_date'], # composite primary key
    'databank': ['id', 'topic']
} # Map your table names to their primary keys

DEFAULT_TABLE = TABLES_TO_SHOW[0] if TABLES_TO_SHOW else None

# Configuration for column widths
# Only 'description' has a fixed width. All other columns will be flexible.
COLUMN_WIDTHS = {
    'user': [('gender', 300)],
}

# New configuration for many-to-many relationships
# Define which tables have a many-to-many relationship via a junction table.
MANY_TO_MANY_CONFIG = {
    'users': {
        'junction_table': 'usr_grp_jct',
        'fk_self': 'uid',
        'fk_other': 'gid',
        'other_table': 'groups',
        'other_display_column': 'name'
    },
    # Add more relationships here as needed
    # 'users': {
    #     'junction_table': 'user_roles',
    #     'fk_self': 'user_id',
    #     'fk_other': 'role_id',
    #     'other_table': 'roles',
    #     'other_display_column': 'role_name'
    # }
}

WRITE_ONLY_CONFIG = {
    'databank': {
        'contributor_column': 'contributor_usernames'
    },
}

# Columns that are read-only for the user (cannot be updated)
# Note: Primary keys are automatically read-only.
READ_ONLY_COLUMNS = {
    'groups': ['creation_date'],
}

# Columns to hide entirely from all views (main, expanded, and forms)
HIDDEN_COLUMNS = {
    'databank': ['id']
}

# Columns to show in the main table view.
# All other columns for this table will be hidden in the main view.
# If a table is not listed, all columns will be shown.
VISIBLE_COLUMNS = {
    'users': ['id', 'username', 'email', 'gender'],
}

FOREIGN_KEY_CONFIG = {
    'users': {
        'group_id': {
            'foreign_table': 'groups',
            'foreign_key': 'id',
            'search_columns': ['name', 'description'],
            'display_columns': ['name', 'description', 'creation_date']
        }
    },
    'databank': {
        'author_id': {
            'foreign_table': 'users',
            'foreign_key': 'id',
            'search_columns': ['username', 'email'],
            'display_columns': ['username', 'email', 'gender']
        }
    }
}