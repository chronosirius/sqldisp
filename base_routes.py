from flask import Blueprint, redirect, render_template, request, session, url_for, send_file
from functions import get_db_connection
from config import DEFAULT_TABLE
from subprocess import run
from json import loads

base_routes = Blueprint('base_routes', __name__)


@base_routes.route('/login', methods=['GET', 'POST'])
def login():
    try:
        k = loads(run(args=["tailscale", "whois", "--json", request.remote_addr], capture_output=True, text=True).stdout)['CapMap']['chronosirius.xyz/pdb'][0]
    except Exception as e:
        print(e)
        k = None

    """Displays the login form on GET and handles login on POST."""
    if request.method == 'POST' or k is not None:
        user = request.form['user'] if k is None else k['username']
        password = request.form['password'] if k is None else k['passkey']

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
    return redirect(url_for('base_routes.login'))


@base_routes.route('/')
def root_redirect():
    if not DEFAULT_TABLE:
        return "No tables are configured to be shown."
    return redirect(url_for('dbview.index', table_name=DEFAULT_TABLE))


@base_routes.route('/favicon.ico')
def favicon():
    return send_file('favicon.ico')