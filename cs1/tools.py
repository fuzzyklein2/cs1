""" c1.tools

    Define functions to be used in the Case Study 1 Jupyter Notebooks.
"""
from cmd import Cmd
from datetime import date, datetime, timedelta
from dateutil.parser import parse
from ftplib import FTP_TLS as FTP
from functools import partial, wraps
from glob import glob
import hashlib
import os
from pathlib import Path
from pprint import pprint as pp
import re
import shutil
import sys
from traceback import print_exc
import warnings
from warnings import warn
from zipfile import ZipFile

warnings.simplefilter("ignore")

from jellyfish import levenshtein_distance as similar
import mysql.connector
from numpy import *
import pandas as pd
import requests

MAX_COLS = 13

BASE_DIR = Path(os.curdir).resolve().absolute()
DATA_DIR = BASE_DIR / 'data'
TEST_DIR = BASE_DIR / 'test'
ARCHIVE  = BASE_DIR / 'archive'
DOWNLOAD = BASE_DIR / 'download'
CLEAN_DIR = BASE_DIR / 'clean'
INDEX_TEXT = BASE_DIR / 'aws_index.txt'
CSV_DIR = BASE_DIR / 'csv'
HEADER_FILE = BASE_DIR / 'header.csv'
STAGED_DIR = BASE_DIR / 'staged'

DT_ZERO = timedelta(0, 0, 0, 0, 0, 0)

def pwd():
    return str(Path(os.curdir).resolve().absolute())

def cd(p: Path):
    os.chdir(p)
    return pwd()

def columnize(l):
    Cmd().columnize(l)

def public(obj):
    columnize([s for s in dir(obj) if not s.startswith('_')])

def head(p):
    with p.open() as f:
        for i in range(20):
            print("'" + f.readline().rstrip('\n') + "'")

def path2str(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        args = [str(a) for a in args]
        return f(*args, **kwargs)
    return wrapper

def geo_deg_2_feet(d):
    return d*1000/9*3280.4

def like(s1, s2):
    return similar(s1, s2) < 2

def hilite_src_lines(obj):
    codeStr = inspect.getsource(obj)
    hilite_params = { "code": codeStr }
    return requests.post(HILITE_ME, hilite_params).text

def reverse_lookup(d:dict, s:str):
    result = None
    for i in d.items():
        if i[0] == s:
            return i[0]
        if type(i[1]) is str:
            if i[1] == s:
                return i[0]
        elif type(i[1]) is list:
            for t in i[1]:
                if t == s:
                    return i[0]
    return None

def rev_lookup(s):
    return reverse_lookup(COLUMNS, s)

def download_data():
    if not DOWNLOAD.exists():
        DOWNLOAD.mkdir()
    cur_dir = pwd()
    os.chdir(DOWNLOAD)
    for url in [s for s in re.compile('<(.*)>').findall(INDEX_TEXT.read_text()) if s.endswith('.zip')]:
        (DOWNLOAD / url.split('/')[-1]).write_bytes(requests.get(url).content)

def extract_data():
    cur_dir = pwd()
    if not DATA_DIR.exists():
        DATA_DIR.mkdir()
    cd(DATA_DIR)
    for f in glob(f'{str(ARCHIVE)}/*.zip'):
        z = ZipFile(f)
        csv_files = [info.filename for info in z.filelist if info.filename.endswith('.csv') and not info.filename.startswith('_')]
        for csv in csv_files:
            z.extract(csv)
    cd(cur_dir)

def move_zip_files():
    if not ARCHIVE.exists():
        ARCHIVE.mkdir()
    for p in [Path(f) for f in glob(f'{str(DOWNLOAD)}/*.zip')]:
        src = str(p)
        dest = str(ARCHIVE / p.name)
        shutil.move(src, dest)

def hash_zip_files():
    cd(ARCHIVE)
    for f in glob('**'):
        # filename = input("Enter the file name: ")
        md5_hash = hashlib.md5()
        with (ARCHIVE/f).open("rb") as input:
            # Read and update hash in chunks of 4K
            for byte_block in iter(lambda: input.read(4096),b""):
                md5_hash.update(byte_block)
            (ARCHIVE/('.'.join(f.split('.')[:-1]) + '.md5')).write_text(md5_hash.hexdigest())
    cd(BASE_DIR)

def refresh_data():
    download_data()
    extract_data()
    move_zip_files()
    hash_zip_files()

def list_files():
    cd(DATA_DIR)
    csv_files = [Path(s).absolute() for s in glob('**', recursive=True) if s.endswith('.csv')]
    station_files = [p for p in csv_files if 'Station' in p.name]
    trip_files = list(set(csv_files).difference(station_files))
    cd(BASE_DIR)
    return csv_files, station_files, trip_files

CSV_FILES, STATION_FILES, TRIP_FILES = list_files()

def db_connect():
    expected_sql_state = None
    actual_sql_state = None
    output = ''
    cnx = None
    cursor = None
    user, password, host = (Path.home()/'.config'/'my.txt').read_text().split(':')
    try:
        cnx = mysql.connector.connect(user=user, password=password, host=host, database='cs1')
        cursor = cnx.cursor()
        print(f'Connected to database cs1 at {host}')
    except mysql.connector.ProgrammingError as e:
        # print('Caught a ProgrammingError!')
        actual_sql_state = e.sqlstate
        output = 'Error: '
        if e.errno == 1049:
            output += e.msg
            expected_sql_state = '42000'
        elif e.errno == 1045:
            output += 'wrong password'
            expected_sql_state = '28000'
        elif e.errno == 1698:
            output += 'bad user name'
            expected_sql_state = '28000'
        else:
            print('Unknown Programming Error!')
            pp(e)
            print(f'{e.errno=}')
            print(f'{e.sqlstate=}')
            print(f'{e.msg=}')
        print(output)
        if actual_sql_state != expected_sql_state:
            print(f'Unexpected SQL state: {actual_sql_state}')
        return None
    except mysql.connector.InterfaceError as e:
        actual_sql_state = e.sqlstate
        output = 'Error: '
        if e.errno == 2003:
            output += e.msg
        else:
            print('Unknown Interface Error!')
            pp(e)
            print(f'{e.errno=}')
            print(f'{e.sqlstate=}')
            print(f'{e.msg=}')
        print(output)
        if actual_sql_state != expected_sql_state:
            print(f'Unexpected SQL state: {actual_sql_state}')
        return None
    return cnx, cursor

def ftp_connect():
    user, password, host = (Path.home()/'.config'/'ftp.txt').read_text().split(':')
    ftp = FTP(host=host)
    ftp.sendcmd(f'USER {user}')
    ftp.sendcmd(f'PASS {password}')
    return ftp

def get_year(p:Path):
    """ Return the year that the file contains data for.
        Files that contain "Station" need to be filtered out.
    """


def get_data(year=2021):
    """ Create and return a dataframe consisting of all the data for a given
        year.
    """
    # Preserve the current working directory.
    prev_wd = Path(os.curdir).resolve().absolute()
    # Move to the data directory.
    cd(DATA_DIR)

    # Create an empty list to store the files for `year` in.
    F = list()

    for p in TRIP_FILES:
        # Determine whether the file contains data for `year`.
        # Assume, for now, that occurrence of the year in the filename constitutes relevance.
        if str(year) in p.stem:
            print(f'Processing: {p.name}')
            F.append(p)
            
    df = pd.DataFrame()
    for p in F:
        df = pd.concat([df, pd.read_csv(p.name)])
            
    cd(prev_wd)
    
    return df


# def choose_name(names):
#     print("WARNING: Station has multiple names!")
#     print()
#     for i, n in enumerate(names):
#         print(f'{i}: {n}')
#     print()
#     i = input("Enter a name to use: ")
#     if i.isdigit():
#         return names[i]
#     else:
#         return i
