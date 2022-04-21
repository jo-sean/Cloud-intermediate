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
        boat_key = client.key(constants.boats, int(bid))
        boat = client.get(key=boat_key)

        # Checks if boat with boat_id exists
        if not boat:
            return {"Error": "No boat with this boat_id exists"}, 404

        # Check to see if load(s) is/are on the boat; remove load(s) (carrier==None)
        query = client.query(kind=constants.loads)
        loads_list = list(query.fetch())

        for curr_load in loads_list:
            if curr_load["carrier"] and curr_load["carrier"]['id'] == bid:
                curr_load.update({"carrier": None})
                client.put(curr_load)

        client.delete(boat_key)

        return "", 204

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


@bp.route('/<bid>/loads/<lid>', methods=['PUT', 'DELETE'])
def put_delete_loads_in_boat(bid, lid):
    if request.method == 'PUT':

        # Gets boat
        boat_key = client.key(constants.boats, int(bid))
        boat = client.get(key=boat_key)

        # Gets load
        load_key = client.key(constants.loads, int(lid))
        load = client.get(key=load_key)

        print(bid, lid, boat, load)

        # Check contents of the json file to make sure slip and boat exists
        if not load or not boat:
            return {"Error": "The specified boat and/or load does not exist"}, 404

        elif load["carrier"]:
            return {"Error": "The load is already loaded on another boat"}, 403

        else:

            boat['loads'].append({"id": lid, "self": request.root_url + "loads/" + str(load.key.id)})
            load['carrier'] = {"id": bid, "name": boat['name'], "self": request.root_url + "boats/" + str(boat.key.id)}

        client.put(boat)
        client.put(load)
        return '', 204

    elif request.method == 'DELETE':

        # Gets load
        load_key = client.key(constants.loads, int(lid))
        load = client.get(key=load_key)

        # Gets boat
        boat_key = client.key(constants.boats, int(bid))
        boat = client.get(key=boat_key)

        if boat:
            boat_check = next((index for index, load in enumerate(boat['loads'])
                               if load['id'] == lid), None)

        # Check contents of the json file to make sure slip, boat exists,
        # and boat is parked at this slip
        if not load or not boat or load["carrier"] is None or \
                load["carrier"]['id'] != bid or boat["loads"] == [] or \
                boat_check is None:
            return {"Error": "No boat with this boat_id is loaded with the load with this load_id"}, 404

        else:
            del boat['loads'][boat_check]
            load["carrier"] = None
            client.put(boat)
            client.put(load)

            return '', 204

    else:
        return 'Method not recognized'


@bp.route('/<bid>/loads', methods=['GET'])
def get_reservations(bid):
    boat_key = client.key(constants.boats, int(bid))
    boat = client.get(key=boat_key)
    load_list = {"self": request.root_url + "boats/" + bid, "loads": []}

    # Check if boat exists
    if not boat:
        return {"Error": "No boat with this boat_id exists"}, 404

    if boat['loads']:
        for load in boat['loads']:
            load_list['loads'].append(load)

        return json.dumps(load_list), 200

    # Boat has no loads
    else:
        return json.dumps([]), 200







