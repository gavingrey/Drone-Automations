import os, time, datetime, requests
import pandas as pd
from dateutil import parser
from PIL import Image
from PIL.ExifTags import TAGS
from stat import *

def _process_dates(cell):
    if cell and type(cell) == str:
        if cell[:4] == '1900':
            return ''
        else:
            return parser.isoparse(cell).astimezone()
        return cell
    else:
        if not type(cell) == str:
            print(type(cell))
        return cell    

def _qb_to_df(headers, body):
    r = requests.post(
        'https://api.quickbase.com/v1/records/query', 
        headers = headers, 
        json = body
    )
    df = pd.json_normalize(r.json()['data'])
    fields = pd.DataFrame(r.json()['fields'])
    fields_mapping = dict(zip(fields['id'], fields['label']))
    df.rename(columns=lambda x: fields_mapping[int(x.replace('.value', ''))], inplace=True)
    df.set_index('Related Site', inplace=True)  
    df = df.applymap(_process_dates)
    df.reset_index(inplace=True)
    return df

def _pull_flights():
    headers = {
        'QB-Realm-Hostname': 'thecbrgroup',
        'Authorization': 'QB-USER-TOKEN b5qarz_i55j_0_cqcsib22aqqtcfrxvsizrdtam'
    }
    body = {
        "from": "bqxyyc6bc",
        "select": [
            104,
            9,
            32
        ],
        "where": "{104.XEX.''}AND{9.XEX.''}AND{32.XEX.''}"
    }
    return _qb_to_df(headers,body)

def _is_panorama(file_entry):
    if file_entry.name[-3:] != 'JPG':
        return False
    image = Image.open(file_entry.path)
    exifdata = image.getexif()
    for tag_id in exifdata:
        tag = TAGS.get(tag_id, tag_id)
        if not tag == 'XPKeywords':
            continue
        data = exifdata.get(tag_id)
        if isinstance(data, bytes):
            data = data.decode()
            data = data.replace('\x00', '')
        return (data == 'pano')
    return None

#Given a DJI file, get its Site ID
def _process_dji(file_entry, flight_info):
    #Grab the filename and creation time
    filename = file_entry.name
    create_time = file_entry.stat().st_birthtime
    datetime_time = datetime.datetime.fromtimestamp(create_time).astimezone()
    #Grab a dataframe with all flight information
    mask = (flight_info['Projected Flight Date'] <= datetime_time) & (datetime_time <= flight_info['Time After Landing'])
    corresponding_sites = flight_info.loc[mask]
    if not corresponding_sites.empty:
        site_id = corresponding_sites.iloc[0]['Related Site']
        panorama = _is_panorama(file_entry)
        if panorama:
            site_id = site_id + "_PANO"
        print(f'{filename}:\t{site_id}')
        return site_id
    if len(corresponding_sites) > 1:
        print(f"TOO MANY SITES FOR {filename}")
    else:
        print(f'{filename}: NOTHING FOUND')
    return

#https://stackoverflow.com/a/64987367
def _merge_dictionary_list(dict_list):
  return {
    k: [d.get(k) for d in dict_list if k in d] # explanation A
    for k in set().union(*dict_list) # explanation B
    }

def _get_site_id_mapping(folderpath, flight_info, recursive=False):
    site_id_mapping = {}
    directory_list = []
    for entry in os.scandir(folderpath):
        mode = entry.stat().st_mode
        if S_ISDIR(mode):
            directory_list.append(entry)
        elif S_ISREG(mode):
            if entry.name[0:3] == 'DJI':
                site_id = _process_dji(entry, flight_info)
                if site_id is None:
                    continue
                if site_id in site_id_mapping:
                    site_id_mapping[site_id].append(entry.path)
                else:
                    site_id_mapping[site_id] = [entry.path]
    if recursive:
        for directory in directory_list:
            new_site_id_mapping = _get_site_id_mapping(directory, flight_info, True)
            site_id_mapping = _merge_dictionary_list([site_id_mapping, new_site_id_mapping])
    return site_id_mapping

def classify_photos(folderpath, recursive=False):
    flight_info = _pull_flights()
    site_id_mapping = _get_site_id_mapping(folderpath, flight_info, True)
    return

if __name__ == '__main__':
    classify_photos('/Volumes/Untitled/DCIM/', True)
    #classify_photos('/Users/gavingrey/Downloads/', True)