import os
import csv
from app.scripts._global import GLOBAL_CONFIG

def get_user_csvfile(csvfile, username):
    dir_users = GLOBAL_CONFIG['users_data']
    csvdir = os.path.join(dir_users, username, 'multipoints')

    msg0 = 'You do not have a saved CSV files on your account.'
    mpts = {'mpts': None, 'message': msg0}

    if not os.path.exists(csvdir):
        return mpts

    if (csvfile is None) or (csvfile == ''):
        return mpts

    csvpath = os.path.join(csvdir, csvfile)
    if not os.path.exists(csvpath):
        msg = f'File {csvfile} not found.'
        mpts = {'mpts': None, 'message': msg}
        return mpts

    csvdata = read_csv_file(csvpath)
    mpts = {'mpts': csvdata, 'message': None}
    return mpts

def read_csv_file(csvfile):
    with open(csvfile, 'r') as fl:
        csvreader = csv.DictReader(fl)
        csvdata = []
        for row in csvreader:
            csvdata = csvdata + [row]

    return csvdata
