from box_jwt import box_service
import os, time, datetime, requests
import pandas as pd
from dateutil import parser
from PIL import Image
from PIL.ExifTags import TAGS
from stat import *
from collections import defaultdict
from cbr_box import CBR_BOX
from cbr_quickbase import CBR_QB

class DronePhotoOrganizer:

    def _is_panorama(self, file_entry):
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
    def _process_dji(self, file_entry, flight_info):
        #Grab the filename and creation time
        filename = file_entry.name
        create_time = file_entry.stat().st_birthtime
        datetime_time = datetime.datetime.fromtimestamp(create_time).astimezone()
        #Grab a dataframe with all flight information
        mask = (self.flight_info['Flight Start Time'] <= datetime_time) & (datetime_time <= self.flight_info['Time After Landing'])
        corresponding_sites = flight_info.loc[mask]
        if not corresponding_sites.empty:
            site_id = corresponding_sites.iloc[0]['Related Site']
            panorama = self._is_panorama(file_entry)
            if panorama:
                site_id = site_id + "_PANO"
            return site_id
        if len(corresponding_sites) > 1:
            print(f"TOO MANY SITES FOR {filename}")
        else:
            print(f'{filename}: NOTHING FOUND')
        return

    #https://stackoverflow.com/a/1495562
    def _merge_dictionary_list(self, a, b):
        de = defaultdict(list, a)
        for i, j in b.items():
            de[i].extend(j)
        return dict(de)

    def _pull_flights(self):
        tableid = "bqxyyc6bc"
        fieldslist = [104,84,32]
        querystring = "{104.XEX.''}AND{84.XEX.''}AND{32.XEX.''}"
        return self.qb_client.query_records(tableid, fieldslist, querystring)

    def _get_site_id_mapping(self, folderpath, flight_info, recursive=False):
        site_id_mapping = {}
        directory_list = []
        for entry in os.scandir(folderpath):
            mode = entry.stat().st_mode
            if S_ISDIR(mode):
                directory_list.append(entry)
            elif S_ISREG(mode):
                if entry.name[0:3] == 'DJI':
                    site_id = self._process_dji(entry, flight_info)
                    if self.logging:
                        print(f'{entry.name}: {site_id}')
                    if site_id is None:
                        continue
                    if site_id in site_id_mapping:
                        site_id_mapping[site_id].append(entry.path)
                    else:
                        site_id_mapping[site_id] = [entry.path]
        if recursive:
            for directory in directory_list:
                new_site_id_mapping = self._get_site_id_mapping(directory, flight_info, recursive=recursive)
                site_id_mapping = self._merge_dictionary_list(site_id_mapping, new_site_id_mapping)
        return site_id_mapping

    def _update_drone_folder_button(self, site_id, folder_id):
        #Field IDs for Site ID and Drone Folder
        field_ids = [6, 27]
        #Box URL
        box_url = f'https://thecbrgroup.app.box.com/folder/{folder_id}'
        #Dataframe for upload
        df = pd.DataFrame([[site_id, box_url]], columns=field_ids)
        #Sites table id
        tableid = 'bq8fc85sy'
        #Update frone folders
        self.qb_client.update_records_with_dataframe(df, tableid)
        return

    def upload_regular_photos(self):
        regular_site_photos = [site_id for site_id in self.site_id_mapping if not 'PANO' in site_id]
        if not regular_site_photos:
            print('No photos to process! Classify photos first!')
            return
        for site_id in regular_site_photos:
            photos_list = self.site_id_mapping[site_id]
            folder_id = self.box_client.get_drone_folder_id(site_id)
            if not folder_id:
                continue
            self.box_client.upload_list(photos_list, folder_id)
            self._update_drone_folder_button(site_id, folder_id)
        return

    def upload_panoramas(self):
        panoramas = [pano_id for pano_id in self.site_id_mapping if 'PANO' in pano_id]
        if not panoramas:
            print('No Panoramas to Process!')
            return
        for pano_id in panoramas:
            photos_list = self.site_id_mapping[pano_id]
            site_id = pano_id.replace('_PANO', '')
            
        return

    def __init__(self, folderpath, recursive=False, logging=False):
        self.root_folder = folderpath
        self.recursive = recursive
        self.logging = logging
        self.box_client = CBR_BOX(logging=logging)
        self.qb_client = CBR_QB('thecbrgroup', 'b5qarz_i55j_0_cqcsib22aqqtcfrxvsizrdtam', logging=logging)
        self.flight_info = self._pull_flights()
        self.site_id_mapping = self._get_site_id_mapping(folderpath, self.flight_info, recursive=recursive)
        return
        


if __name__ == '__main__':
    classify = DronePhotoOrganizer('/Volumes/Untitled/DCIM/', recursive=True, logging=True)
    classify.upload_regular_photos()
    #classify.upload_regular_photos(logging=True)
    #classify_photos('/Volumes/Untitled/DCIM/', True)
    #classify_photos('/Users/gavingrey/Downloads/', True)