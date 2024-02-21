from dotenv import find_dotenv, load_dotenv
import os
from pymongo import MongoClient
from bson.objectid import ObjectId

load_dotenv(find_dotenv())

password = os.environ.get('MONGODB_PWD')
connection_string = f"mongodb+srv://gustavotigde:{password}@demo.jlkippi.mongodb.net/"

client = MongoClient(connection_string)

dbs = client.list_database_names()
test_db = client['TEST_DB']
collections = test_db.list_collection_names()

#test inesrting a single document to a collection - insert_one()
def insert_test_doc():
    collection = test_db['TEST_COLLECTION']
    test_doc = {
        "name": "Gustav",
        "type": "Test"
    }
    inserted_id = collection.insert_one(test_doc).inserted_id
    print(inserted_id)


production = client['production']
person_collection = production['person_collection']


#populating many docs to a collection - insert_many()
def create_documents():
    first_names = ['Tim', 'John', 'Sara', 'Gustavo', 'Brahano', 'Daniel', 'Shyla', 'Sam', 'Billy']
    last_names = ['Who', 'Smith', 'Johns', 'Tigde', 'Taganya', 'Klemton', 'YouKnowWho', 'Strogonowich', 'No-Mates']
    ages = [31, 44, 52, 25, 19, 67, 45, 66, 15]
    docs = []

    for first, last, age in zip(first_names, last_names, ages):
        doc = {"first_name": first, "last_name": last, "age": age}
        docs.append(doc)

    person_collection.insert_many(docs)


# find documents using find()
def find_all_people():
    people = person_collection.find()

    for person in people:
        print(f'{person = }')

# find document using find_one()
def find_gustav():
    gustav = person_collection.find_one({"first_name": "Gustavo"})
    print(f'{gustav = }')


# count documents - count_documents()
def count_all_people():
    count = person_collection.count_documents(filter={})
    print(f'count of people: {count}')    


# get document by id
def get_person_by_id(person_id):
    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    print(f'{person = }')

# conditional find using query 
def get_age_range(min_age, max_age):
    query = {
        "$and": [
            {"age": {"$gte": min_age}},
            {"age": {"$lte": max_age}}
            ]
        }
    people = person_collection.find(query).sort("age")
    for person in people:
        print(f'{person = }')

# select specific columns
def project_columns():
    columns = {"_id": 0, "first_name": 1, "last_name": 1}
    people = person_collection.find({}, columns)
    for person in people:
        print(f'{person = }')

def update_person_by_id(person_id):
    _id = ObjectId(person_id)

    all_updates = {
        "$set": {"new_field": True},
        "$inc": {"age": 1},
        "$rename": {"first_name": "first", "last_name": "last"}
    }

    person_collection.update_one({"_id": _id}, all_updates)
    # for unsetting - person_collection.update_one({"_id": _id}, {"$unset": {"new_field": ""}})    
    

def replace_one(person_id):
    _id = ObjectId(person_id)

    new_doc = {
        "first_name": "new first name",
        "last_name": "new last name",
        "age": 100
    }

    person_collection.replace_one({"_id": _id}, new_doc)

def delete_doc_by_id(person_id):
    _id = ObjectId(person_id)
    person_collection.delete_one({"_id": _id})
    # for deleting many - person_collection.delete_many({})

# --------------------------------------------
    
address = {
    "street": "Bay Street",
    "number": 2356,
    "city": "San Francisco",
    "state": "CA",
    "country": "United States",
    "zip": 48213,
}

address2 = {
    "street": "Mountain Road",
    "number": 4444,
    "city": "San Jose",
    "state": "CA",
    "country": "United States",
    "zip": 48275,
}

def add_address_embed(person_id, address):
    _id = ObjectId(person_id)
    person_collection.update_one({"_id": _id}, {"$addToSet": {"addresses": address}})


def add_address_relationship(person_id, address):
    _id = ObjectId(person_id)
    address = address.copy()
    address['owner_id'] = _id

    address_collection = production['address']
    address_collection.insert_one(address)
   






