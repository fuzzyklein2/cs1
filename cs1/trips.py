from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from tools import *

COLUMNS = dict() # Manually added the `unique_trip_cols`, sorted by correspondence.
COLUMNS['ID'] = ['01 - Rental Details Rental ID', 'ride_id', 'trip_id']
COLUMNS['Start Time'] = ['01 - Rental Details Local Start Time', 'started_at', 'start_time', 'starttime']
COLUMNS['End Time'] = ['01 - Rental Details Local End Time', 'ended_at', 'end_time', 'stoptime']
COLUMNS['Bike ID'] = ['01 - Rental Details Bike ID', 'bikeid']
COLUMNS['Duration'] = ['01 - Rental Details Duration In Seconds Uncapped', 'tripduration']
COLUMNS['From Station ID'] = ['from_station_id', 'start_station_id', '03 - Rental Start Station ID']
COLUMNS['To Station ID'] = ['02 - Rental End Station ID', 'end_station_id', 'to_station_id']
COLUMNS['User Type'] = ['User Type', 'member_casual', 'usertype']
COLUMNS['Gender'] = ['Member Gender', 'gender']
COLUMNS['Birth Year'] = ['05 - Member Details Member Birthday Year', 'birthyear', 'birthday']
COLUMNS['To Station Name'] = ['02 - Rental End Station Name', 'end_station_name', 'to_station_name']
COLUMNS['From Station Name'] = ['03 - Rental Start Station Name', 'from_station_name', 'start_station_name']
COLUMNS['End Latitude'] = ['end_lat']
COLUMNS['End Longitude'] = ['end_lng']
COLUMNS['Start Latitude'] = ['start_lat']
COLUMNS['Start Longitude'] = ['start_lng']
COLUMNS['Bike Type'] = ['rideable_type']

COLS_LIST = COLUMNS.keys() # Columns to keep, redundantly defined below.

USER_VALS = dict()
USER_VALS['M'] = ['Subscriber', 'member']
USER_VALS['C'] = ['Customer', 'casual']
USER_VALS['D'] = ['Dependent']

COLS_2_KEEP = [ # Redundant, but here for the purpose of commenting some out.
               'ID',
               'Start Time',
               'End Time',
               'Bike ID',
               'Duration',
               'From Station ID',
               'To Station ID',
               'User Type',
               'Gender',
               'Birth Year',
               'To Station Name',
               'From Station Name',
               'End Latitude',
               'End Longitude',
               'Bike Type',
               'Start Latitude',
               'Start Longitude'
              ]

def list_trip_files(src=DATA_DIR):
    """ Returns a `list` of any bike trip files in `src`. Only checks to see that the file name does not contain "Station".
    """
    cd(src)
    if type(src) is str:
        src = Path(src)
    value = [src / s for s in glob('**', recursive=True) if s.endswith('.csv') and not 'Station' in s.split('/')[-1]]
    cd(BASE_DIR)
    return value

def read_trip_csv_frame(f):
    """ Return a `pd.DataFrame` representing the `CSV` file `f`. Assumes that `f` lacks column headers.
    """
    if type(f) is Path:
        f = str(f)
    h = pd.read_csv(HEADER_FILE)
    ff = pd.read_csv(f)
    ff.columns = h.columns
    return pd.concat([h, ff])

def unique_values(s):
    """ Return a list containing the unique values contained in the given column over all the trip files.
    """
    v = list()
    for f in list_trip_files():
        df = pd.read_csv(f)
        if s in df.columns:
            for u in df[s].dropna().unique():
                v.append(u)
    v = list(set(v))
#    v.sort()
    if '' in v:
        v = v.remove('')
    return v

def find_trip_cols():
    """ Return a list of all the column names in all of the trip files.
    """
    trip_cols = list()
    for p in list_trip_files():
        with p.open() as f:
            new_list = [s.strip() for s in next(f).strip().split(',')]
            trip_cols.extend(new_list)
    return list(set([str(L) for L in trip_cols]))

def unique_trip_cols():
    """ Return a `list` of the unique column names over all of the trip files.
    """
    u = pd.Series(find_trip_cols()).unique()
    return [u[i] for i in range(len(u))]

def consist_cols(dest=DATA_DIR):
    """ Copy all of the trip files in the `data` directory to the `test` directory with consistent column names.
    """
    log = BASE_DIR / 'trip_cols.log'
    with log.open(mode='w') as f:
        write = partial(print, file=f)
        for i, p in enumerate(list_trip_files()):
            write(f'{i=}', file=f)
            write(f'Processing {p}', file=f)
            lines = p.read_text().split('\n')
            write(f'Columns: {lines[0]}', file=f)
            C = lines[0].split(',')
            output = lines[0]
            for s in C:
                if s in COLS_LIST:
                    s2 = s
                else:
                    if s.startswith('"'):
                        s2 = '"' + reverse_lookup(COLUMNS, s.strip('"')) + '"'
                    else:
                        s2 = reverse_lookup(COLUMNS, s)
                output = output.replace(s, s2 if s2 else '')
            write(f'New Columns: {output}', file=f)
            write(file=f)
            (dest/p.name).write_text('\n'.join([output, *lines[1:]]))

@path2str
def open_data_frame(p):
    """ Return a `pd.DataFrame` representing the file represented by `p`.
    """
    write = partial(print, file=f)
    cols_2_keep = COLS_2_KEEP
    print(f'Processing file: {p.name}')
    df = pd.read_csv(p)
    print(f'{df.columns=}')
    MAX_LEN = len(COLS_2_KEEP)
    cols_2_drop = list(set(df.columns).difference(cols_2_keep))
    print('Columns 2 Drop: ', cols_2_drop)
    df = df.drop(columns=cols_2_drop).reindex(columns=cols_2_keep)
    if len(df.columns) > MAX_LEN:
        df.drop(df.columns[MAX_LEN:], axis=1, inplace=True)
    print('New Columns: ', df.columns)
    return df

@path2str
def read_trips_frame(f):
    return pd.read_csv(f)

def fix_trip_file_names():
    """ Extract each month's trip data from whatever file it is found in. Then
        put that data into a file whose name is formatted as YYYY-MM-ride-data.csv.
    """
    for p in list_trip_files():
        print(f'Processing {str(p)}')
        df = pd.read_csv(str(p))
        print('Checking for null start times')

def profile_trip_files():
    pass
