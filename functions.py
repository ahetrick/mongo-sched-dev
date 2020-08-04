import csv
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date, time

def daterange(start_date, end_date):
    weekdays = []
    delta = timedelta(days=1)
    d = start_date
    diff = 0
    weekend = set([5, 6])
    while d <= end_date:
        if d.weekday() not in weekend:
            weekdays.append(d)
            diff += 1
        d += delta
    for i in weekdays:
        yield i

def make_shifts():
    shifts = []
    hours = [i for i in np.arange(8, 18.5, .5)]
    change_hours = [str(int(i)) + ':00' if float.is_integer(i) else str(int(i)) + ':30' for i in hours]
    make_time = [datetime.strptime(i, '%H:%M').time() for i in change_hours]
    start_date = date(2020, 8, 1)
    end_date = date(2020, 9, 1)
    for single_date in daterange(start_date, end_date):
        for single_time in make_time:
            shifts.append((datetime.combine(single_date, single_time), single_date.isocalendar()[1]))
    return shifts

def csv_to_dict():
    all_csvs = {}
    for i in ['users','locations']:
        all_rows = []
        df = pd.read_csv(r'./{}.csv'.format(i))
        for index, row in df.iterrows():
            all_rows.append(row.to_dict())
        all_csvs[i] = all_rows
    return all_csvs

def make_collections(mdb, dicts):
    shifts = make_shifts()
    for key in dicts.keys():
        coll = mdb[key]
        ids = coll.insert_many(dicts[key])
        if ids:
            print('Collection {} created'.format(key))
    list_shift_dicts = []
    #no shifts.csv; read directly from make_shifts() list to turn into dictionary, then insert into shifts collection
    for i in shifts:
        shift_dict = {}
        make_list = list(i)
        shift_dict['datetime'],shift_dict['week'] = make_list[0], make_list[1]
        list_shift_dicts.append(shift_dict)
    coll = mdb['shifts']
    id = coll.insert_many(list_shift_dicts)
    if id:
        print('Collection {} created'.format('shifts'))
    return mdb

def drop_collections(mdb):
    for key in mdb.list_collection_names():
        coll = mdb[key]
        dropped = coll.drop()
        print('Collection {} dropped'.format(key))
    return mdb

def confirm_creation(cli, mdb):
    dblist = cli.list_database_names()
    if ('scheduler_db' in dblist) and (set(mdb.list_collection_names()) == set(['users', 'locations', 'shifts'])):
        print('The database and all collections exist.')

def show_current_dates(mdb):
    current_shifts = []
    #show shifts 1 hour from now
    today = datetime.today().replace(minute=0).replace(second=0).replace(microsecond=0)
    for doc in mdb.shifts.find( {"datetime": {'$gte': today} }):
        current_shifts.append(doc)
    df = pd.DataFrame(current_shifts)
    df.to_csv('current_dates.csv',index=False)

def check_user_appt(mdb, person_id, shift_id, location_id):
    #check user and appt ids valid
    user = mdb.users.find_one(person_id)
    if not user:
        return("No user")
    appt = mdb.shifts.find_one(shift_id)
    if not appt:
        return("No shift")
    location = mdb.locations.find_one(location_id)
    if not location:
        return("No location")
    #check that user has not already claimed this specific shift
    user_appt_duplicate = mdb.users.find_one( {"$and": [{"_id":user["_id"]},
                                            {"appointments.appointment_id":appt["_id"]},
                                            {"appointments.location_id":location["_id"]}] } )
    if user_appt_duplicate:
        return("Duplicate")
    users_with_appt = mdb.users.find( {"$and": [{"appointments.appointment_id":appt["_id"]}, 
                                                {"appointments.location_id":location["_id"]}] } )
    count_users_with_appt = len([user for user in users_with_appt])
    if count_users_with_appt == 60:
        return("Max capacity appt")
    #count appts for that user during that week; only 2/same week allowed
    user_week = mdb.users.find_one( {"$and": [{"_id":user["_id"]},{"appointments.week": appt["week"]}] } )
    if user_week:
        count_week = len([i['week'] for i in user_week["appointments"] if i['week'] == appt["week"]])
        if count_week == 2:
            return("Max capacity week")
    else:
        pass

def make_user_appt(mdb, shift_id, location_id):
    full_shift = mdb.shifts.find_one(shift_id)
    full_shift["appointment_id"] = full_shift.pop("_id")
    full_location = mdb.locations.find_one(location_id)
    full_location["location_id"] = full_location.pop("_id")
    full_shift.update(full_location)
    return full_shift

def find_max_appts_locations(mdb):
    result = mdb.users.aggregate([
        {"$group" : {
            "_id": {
                "appt": "$appointments.appointment_id",
                "location": "$appointments.location_id",
                "datetime": "$appointments.datetime",
                "location": "$appointments.name"
                },
            "count": {"$sum":1}
             }
        },
        {"$match": {
            "count": {"$gte": 60}
            }
        }
    ])
    #output to csv
    list_of_dicts = []
    n = {}
    for i in result: 
        if i['_id']:
            n['datetime'] = i['_id']['datetime'][0]
            n['location'] = i['_id']['location'][0]
            list_of_dicts.append(n)
    df = pd.DataFrame(list_of_dicts)
    df.to_csv('closed_appts.csv',index=False)
    return result

def find_open_appts_locations(mdb):
    result = mdb.users.aggregate([
        {"$group" : {
            "_id": {
                "appt": "$appointments.appointment_id",
                "location": "$appointments.location_id",
                "datetime": "$appointments.datetime",
                "location": "$appointments.name"
                },
            "count": {"$sum":1}
             }
        },
        {"$match": {
            "count": {"$lte": 60}
            }
        }
    ])
    #output to csv
    list_of_dicts = []
    n = {}
    for i in result: 
        if i['_id']:
            n['datetime'] = i['_id']['datetime'][0]
            n['location'] = i['_id']['location'][0]
            list_of_dicts.append(n)
    df = pd.DataFrame(list_of_dicts)
    df.to_csv('open_appts.csv',index=False)
    return result