from google.cloud import datastore
from flask import Flask, request
import json
import constants
import boats
import loads

app = Flask(__name__)
app.register_blueprint(boats.bp)
app.register_blueprint(loads.bp)


@app.route('/')
def index():
    return "Please navigate to /boats or /loads to use this API"


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
