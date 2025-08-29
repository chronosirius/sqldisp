from flask import Blueprint
from .contrib import contrib
from .fk import fk
from .jct import jct
from .row import row

dbmod = Blueprint('dbmod', __name__)

dbmod.register_blueprint(contrib)
dbmod.register_blueprint(fk)
dbmod.register_blueprint(jct)
dbmod.register_blueprint(row)