import pysftp
import os
from pathlib import Path

class CBR_SFTP:
    def __init__(self, server, username, password, directory):
        sftp = pysftp.Connection(server, username=username, password=password)
        sftp.chdir(directory)
        self.client = sftp

    def __del__(self):
        self.client.close()
        
    def _upload_single_panorama(self, filepath, site_id):
        client = self.client
        filename = Path(filepath).name
        if filename in [i for i in client.listdir(client.pwd)]:
            client.remove(filename)
        new_filename = f'{site_id}.JPG'
        if new_filename in [i for i in client.listdir(client.pwd)]:
            print(f'Panorama already exists for {site_id}!')
            return
        client.put(filepath)
        client.rename(filename, new_filename)
        return

    def upload_panorama_list(self, pano_list, site_id):
        for pano in pano_list:
            self._upload_single_panorama(pano, site_id)
        return

if __name__ == '__main__':
    sftp = CBR_SFTP('128.199.10.227', username='root', password='testpassword', directory='/var/www/gavingrey.dev/public_html/Panoramas')
    sftp._upload_single_panorama('DJI_0025_PANO.JPG', 'TEST2')