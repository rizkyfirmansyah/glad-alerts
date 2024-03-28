"""
A Python script for downloading all files under a folder in Google Drive.
Downloaded files will be saved at the current working directory.

This script uses the official Google Drive API (https://developers.google.com/drive).
As the examples in the official doc are not very clear to me,
so I thought sharing this script would be helpful for someone.

To use this script, you should first follow the instruction 
in Quickstart section in the official doc (https://developers.google.com/drive/api/v3/quickstart/python):
- Enable Google Drive API 
- Download `credential.json`
- Install dependencies

Notes:
- This script will only work on a local environment, 
  i.e. you can't run this on a remote machine
  because of the authentication process of Google.
- This script only downloads binary files not google docs or spreadsheets.

"""

import os.path
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from google.oauth2 import service_account
from retry import retry
import multiprocessing

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']

def authenticate(key_iam):
    credentials_file = key_iam
    credentials = service_account.Credentials.from_service_account_file(credentials_file, scopes=SCOPES)
    return credentials

@retry(tries=10, delay=1, backoff=2)
def fetch_files(files, out_path, drive_service):
    for file in files:
        file_id = file['id']
        file_name = os.path.join(out_path, file['name'])
        request = drive_service.files().get_media(fileId=file_id)
        fh = open(file_name, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        
        try:
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"Downloading {file_name}: {int(status.progress() * 100)} %")
        finally:
            fh.close()
            # delete the files right after downloaded
            drive_service.files().delete(fileId=file['id']).execute()
            print(f"Deleted file with ID: {file['id']}")

@retry(tries=10, delay=3, backoff=2)
def download_files_in_folder(drive_folder, drive_service, out_path, pool):
    folder_id = None
    results = drive_service.files().list(q=f"name='{drive_folder}'", fields="files(id)").execute()
    files = results.get('files', [])

    if not files:
        print(f"No files found in the folder {drive_folder}")
    else:
        if files:
            folder_id = files[0]['id']
            # list all files in the folder
            page_token = None
            # Create a directory for downloaded files
            if not os.path.exists(out_path):
                os.mkdir(out_path)
            while True:
                results = drive_service.files().list(q=f"'{folder_id}' in parents", fields="nextPageToken, files(id, name)", pageToken=page_token).execute()
                _files = results.get('files', [])
                if not _files:
                    print(f"No files found in the folder {drive_folder}")
                    break
                else:
                    pool.map(fetch_files(_files, out_path, drive_service), len(_files))
        
                    page_token = results.get('nextPageToken')
                    if not page_token:
                        break
                    
@retry(tries=20, delay=3, backoff=2)
def download_files_from_gdrive(key_iam, drive_folder, out_path):
    """
        Args:
            out_path: string
            Specifying your output path folder to download all files
            
        Returns: void
        
    """
    credentials = authenticate(key_iam)
    if not credentials:
        print("Authentication to Google Drive failed")
        return

    drive_service = build('drive', 'v3', credentials=credentials)
    try:
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
        download_files_in_folder(drive_folder, drive_service, out_path, pool)
    finally:
        pool.close()
        pool.join()