from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import os
import re

# Scopes nécessaires pour lire dans Google Drive
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

def authenticate():
    """ Authentifie l'utilisateur et renvoie le service Google Drive """
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    
    service = build('drive', 'v3', credentials=creds)
    return service

def find_folder_id(service, folder_name):
    """ Retourne l'ID d'un dossier donné par son nom. """
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
    results = service.files().list(q=query, fields="files(id)").execute()
    items = results.get('files', [])
    
    if not items:
        print(f"Aucun dossier trouvé avec le nom: {folder_name}")
        return None
    return items[0]['id']

def list_folders_starting_with_digit(service, parent_folder_id):
    """ Retourne la liste des sous-dossiers dans un dossier donné qui commencent par un chiffre. """
    query = f"'{parent_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder'"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])
    
    folders_starting_with_digit = []
    
    for item in items:
        if re.match(r'^\d', item['name']):
            folders_starting_with_digit.append(item)
    
    return folders_starting_with_digit

def main():
    folder_name = 'Web-Formations Harmonie Santé'  # Nom du dossier à rechercher
    service = authenticate()

    # Récupérer l'ID du dossier par son nom
    folder_id = find_folder_id(service, folder_name)

    if folder_id:
        # Lister les sous-dossiers dans le dossier et filtrer ceux qui commencent par un chiffre
        folders_to_list = list_folders_starting_with_digit(service, folder_id)

        if folders_to_list:
            print("Dossiers commençant par un chiffre:")
            for folder in folders_to_list:
                print(f"Nom: {folder['name']}, ID: {folder['id']}")
        else:
            print("Aucun dossier ne commence par un chiffre.")

if __name__ == '__main__':
    main()
