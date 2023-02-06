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

import io
import pickle
import os.path
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

# ID of the folder to be downloaded.
# ID can be obtained from the URL of the folder
FOLDER_ID = '1gRBRE2qdhOo0Jqse3Hmzu0zTTn6S8hJp' # example of drive ID
FILE_DIR = os.getcwd()

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']

def get_service(api_name, api_version, scopes, key_file_location):
    """
    Get a service to communicates with Google API
    Args:
        api_name: The name of the api to connect to.
        api_version: The api version to connect to.
        scopes: A list auth scopes to authorize for the application.
        key_file_location: The path to a valid service account JSON key file.

    Returns:
        A service that is connected to the specified API.
    """
    credentials = service_account.Credentials.from_service_account_file(key_file_location)
    scoped_credentials = credentials.with_scopes(scopes)

    # Build the service object
    service = build(api_name, api_version, credentials=scoped_credentials)

    return service


def main():
    """
    Download all files in the specified folder in Google Drive
    """
    creds = None
    keyfile = os.path.join(os.getcwd(), 'gee-service.json')
    try:
        service = get_service(api_name='drive', api_version='v3', scopes=SCOPES, key_file_location=keyfile)

    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f'An error occurred: {error}')

    page_token = None
    
    while True:
        folderid = service.files().get(fileId=FOLDER_ID).execute()['id']
        print(service.files().list().execute().get('files', []))

        results = service.files().list(
            q=f"'{FOLDER_ID}' in parents",
            pageSize=10,
            fields="nextPageToken, files(id, name)",
            pageToken=page_token).execute()
        items = results.get('files', [])

        if not items:
            print("No files found.")
        else:
            for item in items:
                file_id = item['id']
                file_name = item['name']
                print(f'{file_name} ({file_id})')
                
                request = service.files().get_media(fileId=file_id)
                with open(FILE_DIR+file_name, 'wb') as fh:
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while done is False:
                        status, done = downloader.next_chunk()
                        print(f'Download {int(status.progress() * 100)}')
                    
                    # delete stored files in Google Drive once it successfully downloaded
                    deletefile = service.files().delete(fileId=file_id).execute()

        page_token = results.get('nextPageToken', None)

        if page_token is None:
            break

if __name__ == '__main__':
    main()