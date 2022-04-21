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

        new_load = datastore.entity.Entity(key=client.key(constants.loads))
        new_load.update({"volume": content["volume"], "carrier": None, "item": content["item"],
                         "creation_date": content["creation_date"]})
        client.put(new_load)

        new_load["id"] = new_load.key.id
        new_load["self"] = request.base_url + str(new_load.key.id)

        return json.dumps(new_load), 201

    elif request.method == 'GET':
        # Get query of loads and set the limit and offset for the query
        query = client.query(kind=constants.loads)
        q_limit = int(request.args.get('limit', '3'))
        q_offset = int(request.args.get('offset', '0'))

        # Get result of query and make into a list
        load_iterator = query.fetch(limit=q_limit, offset=q_offset)
        pages = load_iterator.pages
        results = list(next(pages))

        # Create a "next" url page using 
        if load_iterator.next_page_token:
            next_offset = q_offset + q_limit
            next_url = request.base_url + "?limit=" + str(q_limit) + "&offset=" + str(next_offset)
        else:
            next_url = None

        # Adds id key and value to each json slip; add next url 
        for load in results:
            load["id"] = load.key.id
        output = {"loads": results}

        if next_url:
            output["self"] = next_url

        return json.dumps(output), 200

    else:
        return 'Method not recognized'


@bp.route('/<lid>', methods=['PUT', 'DELETE', 'GET'])
def loads_get_put_delete(lid):
    if request.method == 'PUT':
        content = request.get_json()
        load_key = client.key(constants.loads, int(lid))
        load = client.get(key=load_key)
        load.update({"name": content["name"]})
        client.put(load)
        return '', 200

    elif request.method == 'DELETE':
        load_key = client.key(constants.loads, int(lid))
        load = client.get(key=load_key)

        # Checks if load with load_id exists
        if not load:
            return {"Error": "No load with this load_id exists"}, 404

        if load['carrier']:
            # Gets boat with load
            bid = load['carrier']['id']
            boat_key = client.key(constants.boats, int(bid))
            boat = client.get(key=boat_key)

            # Find index in loads list where lid is located
            boat_check = next((index for index, load in enumerate(boat['loads'])
                               if load['id'] == lid), None)

            # Delete load from loads in the boat
            del boat['loads'][boat_check]
            client.put(boat)

        return '', 204

    elif request.method == 'GET':
        load_key = client.key(constants.loads, int(lid))
        load = client.get(key=load_key)

        # Check if load exists
        if not load:
            return {"Error": "No load with this load_id exists"}, 404

        load["id"] = load.key.id
        load["self"] = request.base_url

        return json.dumps(load), 200

    else:
        return 'Method not recognized'
