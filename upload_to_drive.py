from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os

# Scopes nécessaires pour lire et écrire dans Google Drive
SCOPES = ['https://www.googleapis.com/auth/drive.file']

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

def upload_file(service, file_path):
    """ Télécharge un fichier sur Google Drive et renvoie l'ID du fichier """
    file_name = os.path.basename(file_path)  # Utiliser le nom d'origine du fichier
    file_metadata = {'name': file_name}
    media = MediaFileUpload(file_path, resumable=True)
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f'Fichier téléchargé avec ID: {file.get("id")}')
    return file.get('id')

def set_public_permission(service, file_id):
    """ Définit le fichier comme public (lecture seule) et renvoie l'URL de partage """
    permissions = {
        'type': 'anyone',
        'role': 'reader'
    }
    service.permissions().create(fileId=file_id, body=permissions).execute()
    file = service.files().get(fileId=file_id, fields='webViewLink').execute()
    return file.get('webViewLink')

def main():
    file_path = 'votre_fichier.txt'  # Remplacez par le chemin du fichier

    service = authenticate()

    # Téléchargement du fichier
    file_id = upload_file(service, file_path)

    # Définir la permission de lecture seule pour tout le monde et obtenir le lien
    shareable_link = set_public_permission(service, file_id)
    print(f'Lien de partage: {shareable_link}')

if __name__ == '__main__':
    main()
