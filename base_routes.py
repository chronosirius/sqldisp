from flask import Blueprint, redirect, render_template, request, session, url_for
from functions import get_db_connection
from config import TABLES_TO_SHOW

base_routes = Blueprint('base_routes', __name__)

DEFAULT_TABLE = TABLES_TO_SHOW[0] if TABLES_TO_SHOW else None

@base_routes.route('/login', methods=['GET', 'POST'])
def login():
    """Displays the login form on GET and handles login on POST."""
    if request.method == 'POST':
        user = request.form['user']
        password = request.form['password']

        try:
            get_db_connection(user, password).close()
            session['db_user'] = user
            session['db_password'] = password
            return redirect(url_for('base_routes.root_redirect'))
        except Exception as e:
            error = f"Login failed: {e}"
            return render_template('login.html', error=error)

    error = request.args.get('error')
    return render_template('login.html', error=error)


@base_routes.route('/logout')
def logout():
    """Logs the user out by clearing the session."""
    session.clear()
    return redirect(url_for('base_routeslogin'))


@base_routes.route('/')
def root_redirect():
    if not DEFAULT_TABLE:
        return "No tables are configured to be shown."
    return redirect(url_for('dbview.index', table_name=DEFAULT_TABLE))


@base_routes.route('/favicon.ico')
def favicon():
    return '', 204