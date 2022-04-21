from flask import Blueprint, request
from google.cloud import datastore
import json
# from json2html import *
import constants

client = datastore.Client()

bp = Blueprint('loads', __name__, url_prefix='/loads')


@bp.route('', methods=['POST', 'GET'])
def loads_get_post():
    if request.method == 'POST':
        content = request.get_json()

        # Check contents of the json file to make sure keys have values, and it is not empty
        if not content or "volume" not in content or "item" not in content or "creation_date" not in content:
            return {"Error": "The request object is missing at least one of the required attributes"}, 400

        new_load = datastore.entity.Entity(key=client.key(constants.boats))
        new_load.update({"volume": content["volume"], "carrier": None, "item": content["item"],
                         "creation_date": content["creation_date"]})
        client.put(new_load)

        new_load["id"] = new_load.key.id
        new_load["self"] = request.base_url + "/" + str(new_load.key.id)

        return json.dumps(new_load), 201

    elif request.method == 'GET':
        query = client.query(kind=constants.loads)
        q_limit = int(request.args.get('limit', '3'))
        q_offset = int(request.args.get('offset', '0'))
        load_iterator = query.fetch(limit=q_limit, offset=q_offset)
        pages = load_iterator.pages
        results = list(next(pages))

        if load_iterator.next_page_token:
            next_offset = q_offset + q_limit
            next_url = request.base_url + "?limit=" + str(q_limit) + "&offset=" + str(next_offset)
        else:
            next_url = None

        for e in results:
            e["id"] = e.key.id
        output = {"loads": results}

        if next_url:
            output["next"] = next_url
        return json.dumps(output)

    else:
        return 'Method not recognized'


@bp.route('/<id>', methods=['PUT', 'DELETE'])
def loads_put_delete(id):
    if request.method == 'PUT':
        content = request.get_json()
        load_key = client.key(constants.loads, int(id))
        load = client.get(key=load_key)
        load.update({"name": content["name"]})
        client.put(load)
        return ('',200)

    elif request.method == 'DELETE':
        key = client.key(constants.loads, int(id))
        client.delete(key)
        return ('',200)

    else:
        return 'Method not recogonized'