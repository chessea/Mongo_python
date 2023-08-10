from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
## Cargar variables de entorno

load_dotenv(find_dotenv())

# Obtener variable de entorno password 
password = os.getenv("MONGODB_PWD")


uri = f"mongodb+srv://chessea:{password}@cluster0.vyk8nit.mongodb.net/?retryWrites=true&w=majority"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


# Obtener las bases de datos existentes
dbs=client.list_database_names()
print(dbs)


# Nombre de las colecciones
test_db = client.test
collections = test_db.list_collection_names()
print(collections)



# Inertar en una collections
def insert_tesdoc():
    collection= test_db.test
    test_documents = {
        "name": "Tim",
        "tipe": "test"
    }
    inserted_id =collection.insert_one(test_documents).inserted_id
    print( inserted_id)

#insert_tesdoc()


# Crea la coleccion aunque no exista previamente
production = client.production
person_collection = production.person_collection


# Insertar varios datos
def create_documents():
    nombres = ["Ana", "Juan", "María", "Carlos", "Luis"]
    apellidos = ["Gómez", "Pérez", "Rodríguez", "López", "Martínez"]
    edades = [25, 32, 28, 40, 22]

    docs=list()

    for nombre, apellido, edad in zip( nombres, apellidos, edades ):
        doc = {
            "nombre": nombre,
            "apellido":apellido,
            "edad": edad
        }
        docs.append(doc)
    person_collection.insert_many(docs)

create_documents()


printer = pprint.PrettyPrinter()

def find_all_people():
    people = person_collection.find()
    # print(list(people)) imprimir
    for person in people:
        printer.pprint(person)


#find_all_people()

#Encontrar a una persona
def find_carlos():
    carlos = person_collection.find_one({"nombre": "carlos".capitalize()})
    printer.pprint(carlos)

#find_carlos()



# Cantidad de doc
def count_all_people():
    #count = person_collection.count_documents(filter={})
    count = person_collection.find().count
    print("Numero de personas", count)

#count_all_people()


# Obtener por id 
def get_persoon_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    pprint.pprint(person)

#get_persoon_by_id("64d520c943f9d0c08af6dd86")

def get_age_range(min_age, max_age):
    query = {
        "$and": [
            {"edad": {"$gte": min_age}},
            {"edad": {"$lte": max_age}}
        ]
    }
    people = person_collection.find(query).sort("edad")
    for person in people:
        printer.pprint(person)

#get_age_range(20, 38)


def proyect_columns():
    columns = {"_id": 0, "nombre": 1, "apellido":1}
    people = person_collection.find({}, columns)
    for person in people:
        printer.pprint(person)

#proyect_columns()



def update_person_by_id(person_id):

    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    """     all_updates={
        "$set": {"new_field": True},
        "$inc":{"edad": 1},
        "$rename":{"nombre":"first","apellido": "last"}
    }
    person_collection.update_one({"_id": _id}, all_updates) """
    #elimina el campo
    person_collection.update_one({"_id":_id}, {"$unset": {"new_field": ""}})

#update_person_by_id("64d520c943f9d0c08af6dd87")

def replace_one(person_id):

    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    new_doc = {
        "nombre": "new first name",
        "apellido": "new last name ",
        "edad": 100

    }

    person_collection.replace_one({"_id":_id}, new_doc)

#replace_one("64d520c943f9d0c08af6dd88")

def delete_doc_by_id(person_id):

    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    person_collection.delete_one({  "_id":_id})


#delete_doc_by_id("64d520c943f9d0c08af6dd88")

def delete_doc(_ids=[]):
    from bson.objectid import ObjectId

    # Convierte cada ID en la lista a un ObjectId
    object_ids = [ObjectId(_id) for _id in _ids]

    # Utiliza $in para borrar documentos que coincidan con los IDs
    person_collection.delete_many({"_id": {"$in": object_ids}})

#delete_doc(_ids=["64d520c943f9d0c08af6dd85", "64d520c943f9d0c08af6dd86"])


# Relaciones



direccion = {
    "calle": "Calle Principal",
    "numero": "123",
    "ciudad": "Ciudad Ejemplo",
    "estado": "Estado Ejemplo",
    "codigo_postal": "12345"
}

def add_address_embed(person_id, address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    person_collection.update_one({"_id": _id}, {"$addToSet": {"addresses": address}})



#add_address_embed("64d520c943f9d0c08af6dd84", direccion)


def add_address_relationship( person_id, address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    direccion =address.copy()
    direccion["owner_id"] = person_id
    address_collection = production.address
    address_collection.insert_one(direccion)
add_address_relationship("64d549f0c134d651eba83341",direccion)