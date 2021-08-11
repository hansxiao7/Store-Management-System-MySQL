from flask import Flask
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

from lsrs import routes
from lsrs import db
