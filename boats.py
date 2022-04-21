from flask import Blueprint, request
from google.cloud import datastore
import json
import constants

client = datastore.Client()

bp = Blueprint('boat', __name__, url_prefix='/boats')


@bp.route('', methods=['POST', 'GET'])
def boats_get_post():
    if request.method == 'POST':
        content = request.get_json()

        # Check contents of the json file to make sure keys have values, and it is not empty
        if not content or "name" not in content or "type" not in content or "length" not in content:
            return {"Error": "The request object is missing at least one of the required attributes"}, 400

        new_boat = datastore.entity.Entity(key=client.key(constants.boats))
        new_boat.update({"name": content["name"], "type": content["type"], "length": content["length"], "loads": []})
        client.put(new_boat)

        new_boat["id"] = new_boat.key.id
        new_boat["self"] = request.base_url + "/" + str(new_boat.key.id)

        return json.dumps(new_boat), 201

    elif request.method == 'GET':
        # Get query of boats and set the limit and offset for the query
        query = client.query(kind=constants.boats)
        q_limit = int(request.args.get('limit', '3'))
        q_offset = int(request.args.get('offset', '0'))

        # Get result of query and make into a list
        boat_iterator = query.fetch(limit=q_limit, offset=q_offset)
        pages = boat_iterator.pages
        results = list(next(pages))

        # Create a "next" url page using 
        if boat_iterator.next_page_token:
            next_offset = q_offset + q_limit
            next_url = request.base_url + "?limit=" + str(q_limit) + "&offset=" + str(next_offset)
        else:
            next_url = None

        # Adds id key and value to each json slip; add next url 
        for boat in results:
            boat["id"] = boat.key.id
        output = {"boats": results}

        if next_url:
            output["self"] = next_url
        return json.dumps(output), 200

    else:
        return 'Method not recognized'


@bp.route('/<bid>', methods=['PUT', 'DELETE', 'GET'])
def boats_get_put_delete(bid):
    if request.method == 'PUT':
        content = request.get_json()
        boat_key = client.key(constants.boats, int(bid))
        boat = client.get(key=boat_key)
        boat.update({"name": content["name"], "description": content["description"],
                     "price": content["price"]})
        client.put(boat)
        return '', 200
    elif request.method == 'DELETE':
        key = client.key(constants.boats, int(bid))
        client.delete(key)
        return '', 200

    elif request.method == 'GET':
        boat_key = client.key(constants.boats, int(bid))
        boat = client.get(key=boat_key)

        # Check if boat exists
        if not boat:
            return {"Error": "No boat with this boat_id exists"}, 404

        boat["id"] = boat.key.id
        boat["self"] = request.base_url

        return json.dumps(boat), 200

    else:
        return 'Method not recognized'


@bp.route('/<bid>/load/<lid>', methods=['PUT', 'DELETE'])
def add_delete_reservation(bid, lid):
    if request.method == 'PUT':
        boat_key = client.key(constants.boats, int(bid))
        boat = client.get(key=boat_key)
        load_key = client.key(constants.loads, int(lid))
        load = client.get(key=load_key)

        if 'load' in boat.keys():
            boat['load'].append(load.id)
        else:
            boat['load'] = [load.id]
        client.put(boat)

        return '', 200

    if request.method == 'DELETE':
        boat_key = client.key(constants.boats, int(bid))
        boat = client.get(key=boat_key)
        if 'load' in boat.keys():
            boat['load'].remove(int(lid))
            client.put(boat)
        return '', 200


@bp.route('/<bid>/load', methods=['GET'])
def get_reservations(bid):
    boat_key = client.key(constants.boats, int(bid))
    boat = client.get(key=boat_key)
    load_list = []

    if 'load' in boat.keys():
        for lid in boat['load']:
            load_key = client.key(constants.loads, int(lid))
            load_list.append(load_key)
        return json.dumps(client.get_multi(load_list))

    else:
        return json.dumps([])
