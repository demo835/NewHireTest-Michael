import json
import os
from pymongo import MongoClient
from bson import ObjectId
import datetime
import pymongo
import bcrypt
from bson import ObjectId

db_uri = os.environ.get("MONGO_DB_URI", "localhost")
db_name = os.environ.get("MONGO_DB_NAME", "new_hire_test")

db = MongoClient(db_uri)[db_name]


def handle_csv_upload(event, context):
    response_body = {
        "numCreated": 0,
        "numUpdated": 0,
        "errors": [],
    }

    # YOUR LOGIC HERE

    user_template = {
        "_id": ObjectId(),
        "name": "",
        "normalized_email": "",
        "manager_id": None,
        "salary": 0,
        "hire_date": datetime.datetime(1, 1, 1),
        "is_active": True,
        "hashed_password": bcrypt.hashpw(b"correct horse battery staple", bcrypt.gensalt()),
    }
    rows = event.split('\n')
    user_data = []

    # Loop through the rows of the csv file
    for x, row in enumerate(rows):
        if x > 0:
            try:
                user_data.append(row.strip())
            except IndexError as error:
                print('Error is: ', error)

    # Loop through the user data
    for x, user in enumerate(user_data):
        if user == "":
            break
        attributes = user.split(",")
        found_user = db.user.find_one({"normalized_email": attributes[1]})

        if found_user is None:
            # Insert new user
            response_body = update_or_insert_user(user_template,attributes,False,response_body)
            handle_chain_of_command(attributes[1], attributes[2])

        else:
            # Update existing user
            response_body = update_or_insert_user(found_user,attributes,True,response_body)
            handle_chain_of_command(attributes[1], attributes[2])

    response = {
        "statusCode": 200,
        "body": json.dumps(response_body)
    }

    return response

def update_or_insert_user(template,attributes,update,response_body):
    template["name"] = attributes[0]
    template["normalized_email"] = attributes[1]
    template["manager_id"] = find_id_by_email(attributes[2])
    try:
        template["salary"] = int(attributes[3])
    except ValueError as error:
        response_body["errors"].append(error.args)
    parse_date = attributes[4].split("/")
    template["hire_date"] = datetime.datetime(int(attributes[4].split("/")[2]), int(attributes[4].split("/")[0]), int(attributes[4].split("/")[1]))

    if update:
        db.user.update_one({"normalized_email":attributes[1]},
            {"$set" : template}
        )
        response_body["numUpdated"] = response_body["numUpdated"] + 1
    else: # insert
        db.user.insert_one(template)
        response_body["numCreated"] = response_body["numCreated"] + 1
        
    return response_body

def handle_chain_of_command(subordinate_email, manager_email):
    coc_template = {
        '_id': ObjectId(),
        'user_id': ObjectId(),
        'chain_of_command': []
    }

    if manager_email is not "":
        found_manager_id = find_id_by_email(manager_email)
        if found_manager_id:
            coc_template["user_id"] = db.user.find_one({"normalized_email":subordinate_email})["_id"]
            coc_template["chain_of_command"].append(found_manager_id)
        db.chain_of_command.insert_one(coc_template)

def find_id_by_email(email):
    if email:
        return db.user.find_one({"normalized_email":email},{"_id":1})["_id"]