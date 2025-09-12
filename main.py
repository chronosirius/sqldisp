from flask import Flask
from dbmod import dbmod
from base_routes import base_routes
from dbview import dbview
from graph import graph

app = Flask(__name__)
app.secret_key = 'your_super_secret_key'

app.register_blueprint(base_routes)
app.register_blueprint(dbmod)
app.register_blueprint(graph)
app.register_blueprint(dbview)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')