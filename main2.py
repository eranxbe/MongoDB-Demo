from dotenv import find_dotenv, load_dotenv
import os
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime as dt
import pprint

load_dotenv(find_dotenv())
printer = pprint.PrettyPrinter()
password = os.environ.get('MONGODB_PWD')
connection_string = f"mongodb+srv://gustavotigde:{password}@demo.jlkippi.mongodb.net/?authSource=admin"

client = MongoClient(connection_string)

production_db = client['production']

def create_book_collection():
    book_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["title", "authors", "publish_date", "type", "copies"],
            "properties": {
                "title": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "authors": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "objectId",
                        "description": "must be objectId and is required"
                    }
                },
                "publish_date": {
                    "bsonType": "date",
                    "description": "must be a date and is required"
                },
                "type": {
                    "enum": ["Fiction", "Non-Fiction"],
                    "description": "must be enum value and is required"
                },
                "copies": {
                    "bsonType": "int",
                    "minimum": 0,
                    "description": "must be a positive integer and is required"
                }
            }
        }
    }

    try:
        production_db.create_collection("book")
    except Exception as e:
        print(e)

    production_db.command("collMod", "book", validator=book_validator)    

def create_author_collection():
    author_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["first_name", "last_name", "date_of_birth"],
            "properties": {
                "first_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "last_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "date_of_birth": {
                    "bsonType": "date",
                    "description": "must be a date and is required"
                }
            }
        }
    }
    try:
        production_db.create_collection("author")
    except Exception as e:
        print(e)

    production_db.command("collMod", "author", validator=author_validator)

def create_data():
    authors = [
        {
            "first_name": "Gustavo",
            "last_name": "Tigde",
            "date_of_birth": dt(1999, 3, 4)
        },
        {
            "first_name": "George",
            "last_name": "Orwell",
            "date_of_birth": dt(1903, 6, 25)
        },
        {
            "first_name": "Herman",
            "last_name": "Melville",
            "date_of_birth": dt(1819, 8, 1)
        },
        {
            "first_name": "F. Scott",
            "last_name": "Fitzgerald",
            "date_of_birth": dt(1896, 9, 24)
        },
    ]

    author_collection = production_db['author']
    authors_ids = author_collection.insert_many(authors).inserted_ids

    books = [
        {
            "title": "MongoDB Advanced Book",
            "authors": [authors_ids[0]],
            "publish_date": dt.today(),
            "type": "Non-Fiction",
            "copies": 5
        },
        {
            "title": "Python For Dummies",
            "authors": [authors_ids[0]],
            "publish_date": dt(22, 1, 17),
            "type": "Non-Fiction",
            "copies": 5
        },
        {
            "title": "Nineteen Eighty Four",
            "authors": [authors_ids[1]],
            "publish_date": dt(1949, 6, 8),
            "type": "Fiction",
            "copies": 30
        },
        {
            "title": "The Great Gatsby",
            "authors": [authors_ids[3]],
            "publish_date": dt(2014, 5, 23),
            "type": "Fiction",
            "copies": 22
        },
        {
            "title": "Moby Dick",
            "authors": [authors_ids[2]],
            "publish_date": dt(1851, 9, 24),
            "type": "Fiction",
            "copies": 100
        },
    ]

    book_collection = production_db['book']
    book_collection.insert_many(books)

def delete_doc_by_id(person_id, collection_name):
    collection = production_db[f'{collection_name}']
    _id = ObjectId(person_id)
    collection.delete_one({"_id": _id})

books_containing_a = production_db['book'].find({"title": {"$regex": "a{1}"}})
# print(list(books_containing_a))


# JOIN
authors_and_books = production_db['author'].aggregate([{
    "$lookup": {
        "from": "book",
        "localField": "_id",
        "foreignField": "authors",
        "as": "books"
    }
}])

# printer.pprint(list(authors_and_books))

authors_book_count = production_db['author'].aggregate([
    {
    "$lookup": {
        "from": "book",
        "localField": "_id",
        "foreignField": "authors",
        "as": "books"
        }
    },
    {
        "$addFields": {
            "total_books": {"$size": "$books"}
        }
    },{
    "$project": {"first_name": 1, "last_name": 1, "total_books": 1, "_id": 0},
    }
])

printer.pprint(list(authors_book_count))
