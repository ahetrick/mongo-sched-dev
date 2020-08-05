import pymongo
from pymongo import MongoClient
import bson
from bson.objectid import ObjectId
from functions import csv_to_dict, make_collections, drop_collections, confirm_creation, make_shifts, show_current_dates, check_user_appt, make_user_appt, find_open_appts_locations, find_max_appts_locations
from datetime import datetime, timedelta, date, time
import pandas as pd

client = MongoClient("localhost",27017)
db = client["scheduler_db"]

## for testing
# if db.list_collection_names():
#     db = drop_collections(db)

## confirm collections exist
if set(db.list_collection_names()) != set(['users', 'locations', 'shifts']):
    #prep data
    list_of_dicts = csv_to_dict()
    #make collections via mass insert
    db = make_collections(db, list_of_dicts)
    #check collections' and db existence
    confirm_creation(client, db)
else:
   #check collections' and db existence
    confirm_creation(client, db)

## make available_dates.csv
show_current_dates(db)

## claim appointment; receive these values from session; hard-coded for demo
## assumes valid ObjectIds passed
id_person = {"_id": ObjectId("5f28a6748fc060ff5939fe55")}
id_appt = {"_id": ObjectId("5f28a6748fc060ff5939ffd5")} 
id_location = {"_id": ObjectId("5f28a6748fc060ff5939fe59")}

count_appt_week = check_user_appt(db, id_person, id_appt, id_location)

if count_appt_week == "No user":
    print("User not found. Please register to use this app.")
elif count_appt_week == "No week":
    print("Appointment not valid. Please contact administrator.")
elif count_appt_week == "Duplicate":
    print("User has already selected this appointment. Please choose a different appointment.")
#make sure appt id has only been selected 60 times or less
elif count_appt_week == "Max capacity appt":
    print("This appointment has reached maximum capacity. Please choose another appointment.")
elif count_appt_week == "Max capacity week": 
    print("You have reached your maximum number of appointments for that week. Please choose another week.")
else:
    user_appt = make_user_appt(db, id_appt, id_location)
    #add appt to user document
    if 'appointments' in db.users.find_one(id_person).keys():
        db.users.update_one(id_person, {"$push": {"appointments":user_appt}})
    else:
        user_appt = [user_appt]
        db.users.update_one(id_person, {"$set": {"appointments":user_appt}})

## show inserted data
print(db.users.find_one(id_person))

## find appts and location combinations that haven't been claimed 60 times
available = find_open_appts_locations(db)

## find appts and location combinations that have been claimed 60 times
closed = find_max_appts_locations(db)

## cancel an appointment; receive these values from session; hard-coded for demo
id_person = {"_id": ObjectId("5f28a6748fc060ff5939fe55")}
id_appt = {"appointment_id": ObjectId("5f28a6748fc060ff5939ffd5")} 
id_location = {"location_id": ObjectId("5f28a6748fc060ff5939fe59")}
answer = 'Yes'

## create one dictionary with id_appt and id_location as keys
id_appt.update(id_location)

if answer == 'Yes':
    db.users.update_one(id_person, 
    {"$pull": {"appointments": id_appt }})
    print("Your appointment has been cancelled.")

## show that appt has been removed from subdocument array
print(db.users.find_one(id_person))