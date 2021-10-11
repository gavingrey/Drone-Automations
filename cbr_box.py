from box_jwt import *
import os

class CBR_BOX:

    def __init__(self, logging=False):
        self.logging = logging
        self.client = box_service()
        self.site_folders = {}
        self.polygon_list = ['58991984595','60559804135','59009057878','73943714220','63721655367','68098417772','66472689676','81976642643','81979876060','81979683960','81976446226','81979765177','81979936481','92504280594','100127926445','17983212560','41181179313','17983226723','17983243886','18444244264','63722589770','17983216966','17983229400','40585627483','87526631483','40585694010','18444199358','18818602610','17983217638','17983235406','18444196933','17983233568','35231181639','17983227514','18821150647','18655161748','17983231640','19208500759','102697946357','18444217342','116391531082','117870605442','118004588638','122333517951','128868869258','128816777922','128817987540','132717954544','138031505280','144896933613']
        self.file_cache = {}

    def upload_single(self, filepath, folderid, delete_original=False):
        filename = os.path.basename(filepath)
        folder = self.client.folder(folderid)
        if not folder in self.file_cache:
            self.file_cache[folder] = []
            for file in folder.get_items():
                self.file_cache[folder].append(file)
        for file in self.file_cache[folder]:
            if filename == file.name:
                if delete_original:
                    file.delete()
                    folder.upload(filepath)
                    if self.logging:
                        print(f'Overwrote {filename}')
                    return True
                else:
                    file.update_contents(filepath)
                    if self.logging:
                        print(f'Updated {filename}')
                    return True
        folder.upload(filepath)
        if self.logging:
            print(f'Uploaded {filename}')
        return True

    def upload_list(self, file_list, folderid, delete_original=False):
        for file in file_list:
            self.upload_single(file, folderid, delete_original)
        return True

    def _check_site_folder(self, folder):
        return folder.parent.id in self.polygon_list

    def _search_site_folder(self, site_id):
        query_results = self.client.search().query(site_id, result_type = 'folder')
        for folder in query_results:
            if folder.name == site_id and self._check_site_folder(folder):
                return folder.id
        return False

    def get_site_folder_id(self, site_id):
        if site_id in self.site_folders.keys():
            return self.site_folders[site_id]
        else:
            folder_id = self._search_site_folder(site_id)
            if folder_id:
                self.site_folders[site_id] = folder_id
                return folder_id
            else:
                print(f"SITE NOT FOUND IN BOX: {site_id}")
                return False

    def _create_drone_folder(self, photos_folder):
        photos_folder.create_subfolder('Drone Photos')
        return [folder for folder in photos_folder.get_items() if folder.object_type == 'folder' and folder.name == 'Drone Photos'][0]

    def get_drone_folder_id(self, site_id):
        main_folder = self.get_site_folder_id(site_id)
        photos_folder = [item for item in self.client.folder(main_folder).get_items() if item.object_type == 'folder' and '03' in item.name]
        error_message = f"Folder not found for {site_id}"
        if not photos_folder:
            print(error_message) if self.logging else None
            return False
        photos_subfolder = [item for item in photos_folder[0].get_items() if item.object_type == 'folder' and 'PHOTO' in item.name.upper()]
        if not photos_subfolder:
            print(error_message) if self.logging else None
            return False
        drone_folder = [item for item in photos_subfolder[0].get_items() if item.object_type == 'folder' and 'DRONE' in item.name.upper()]
        if not drone_folder:
            drone_folder = [self._create_drone_folder(photos_subfolder[0])]
        return drone_folder[0].id

if __name__ == '__main__':
    box = CBR_BOX()
    site_folder = box.get_drone_folder_id('BA61116S')
    print(site_folder)