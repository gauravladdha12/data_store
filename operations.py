from flask.views import MethodView
from flask import jsonify, request
import json
import fcntl
import msvcrt
import threading
from os import path
from datetime import datetime, timedelta
from dateutil.parser import parse
from app import default_name

def check_time_to_live(self, value):
        # Checks how long the data is accessible.

        created_time = value['CreatedAt']

        # Parse the datetime from the string date.
        created_time = parse(created_time)

        time_to_live = value['Time-To-Live']

        if time_to_live is not None:
            # Calculate the data expire time.
            expired_datetime = created_time + timedelta(seconds=time_to_live)

            # Calculate the remaining seconds of expired time(may/may not expired) from current time.
            remaining_seconds = (expired_datetime - datetime.now()).total_seconds()

            if remaining_seconds <= 0:
                return False

        return value
class CreateData(MethodView):
    def check_data(self,json_data,dbb_path):
        data_obj = json.dumps(json_data)
        if len(data_obj) > 1000000000:
            return False, "DataStore limit will exceed 1GB size."
        for key, value in json_data.items():
            # Check for key in data for 32 length.
            if len(key) > 32:
                return False, "Error key length greater than 32 characters length."

            # Check for value in data whether it is JSON object or not.
            if not isinstance(value, dict):
                return False, "Error data must be in JSON"

            value_obj = json.dumps(value)

            # Check for value JSON object is 16KB or less in size.
            if len(value_obj) > 16384:
                return False, "Error values must be in 16KB size."
        
        datastore = path.join(dbb_path, default_name)
        data = {}
        if path.isfile(datastore):
            with open(datastore) as f:
                # single process only allowed to access the file at a time.
                fcntl.flock(f, fcntl.LOCK_EX)
                data = json.load(f)
                # Releasing the file lock.
                fcntl.flock(f, fcntl.LOCK_UN)

                # Check if file size exceeded 1GB size.
                prev_data_obj = json.dumps(data)
                if len(prev_data_obj) >= 1000000000:
                    return False, "File Size Exceeded 1GB."
        for key in json_data.keys():
            if key in data.keys():
                return False, "Key already exist ."
        
        with open(datastore, 'w+') as f:
            # single process only allowed to access the file at a time..
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(data, f)
            # Releasing the file lock.
            fcntl.flock(f, fcntl.LOCK_UN)

        return True, "Data created in DataStore."

    def __init__(self, db_path):
        self.db_path = db_path

    def POST(self):
        try:
            data = request.get_json(force=True)
        except Exception:
            return jsonify({"status": "error", "message": "Incorrect data format"}), 400

        # Create/Push data into the datasource.
        valid_data, message = check_data(data, self.db_path)
        if not valid_data:
            return jsonify({"status": "error", "message": message}), 400

        return jsonify({"status": "success", "message": message}), 200


class ReadData(MethodView):
    def check_read(self,data_key,dbb_path):
        datastore = path.join(dbb_path, default_name)

        # Check for datastore existance.
        if not path.isfile(datastore):
            return False, "Error no datastore exist"

        # Read previous datastore data if exists.
        with open(datastore) as f:
            # Make sure single process only allowed to access the file at a time.
            # Locking file.
            fcntl.flock(f, fcntl.LOCK_EX)
            data = json.load(f)
            # Releasing the file lock.
            fcntl.flock(f, fcntl.LOCK_UN)

        # Check for the input key available in data.
        if data_key not in data.keys():
            return False, "No data found for the key provided."

        # Check for the data for the key is active or inactive.
        target = data[data_key]
        target_active = check_time_to_live(target)
        if not target_active:
            return False, "Requested data is expired for the key."

        
        store_data=data[data_key]
        del store_data['CreatedAt']
        return True, store_data

    def __init__(self, db_path):
        self.db_path = db_path

    def get(self):
        key = request.args.get('key')
        if key is None:
            return jsonify({"status": "error", "message": "key is not present"}), 400

        # Read data from the datasource with the key(data index).
        data_found, message = check_read(key, self.db_path)
        if not data_found:
            return jsonify({"status": "error", "message": message}), 404

        return jsonify(message), 200


class DeleteData(MethodView):
    def check_delete(self,data_key,dbb_path):
        datastore = path.join(dbb_path, default_name)

        # Check for datastore existance.
        if not path.isfile(datastore):
            return False, "Error no datastore exist"

        # Read previous datastore data if exists.
        with open(datastore) as f:
            # Make sure single process only allowed to access the file at a time.
            # Locking file.
            fcntl.flock(f, fcntl.LOCK_EX)
            data = json.load(f)
            # Releasing the file lock.
            fcntl.flock(f, fcntl.LOCK_UN)

        # Check for the input key available in data.
        if data_key not in data.keys():
            return False, "No data found for the key provided."

        # Check for the data for the key is active or inactive.
        target = data[data_key]
        target_active = check_time_to_live(target)
        if not target_active:
            return False, "Requested data is expired for the key."

        datastore = path.join(dbb_path, default_name)
        #store_data=data[data_key]
        #deleting data from store
        del data[data_key]
        # Write the new data to the datasource after data deletion.
        with open(datastore, 'w+') as f:
            # single process only allowed to access the file at a time.
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(data, f)
            # Releasing the file lock.
            fcntl.flock(f, fcntl.LOCK_UN)

        return True, "Data is deleted from the datastore."

    def __init__(self, db_path):
        self.db_path = db_path

    def delete(self):
        key = request.args.get('key')

        if key is None:
            return jsonify({"status": "error", "message": "key is not present."}), 400

        # Deletes a data from the datasource with the key(data index).
        data_found, message = check_delete(key, self.db_path)
        if not data_found:
            return jsonify({"status": "error", "message": message}), 404

        return jsonify({"status": "success", "message": message}), 200
