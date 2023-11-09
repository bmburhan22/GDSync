import os
import time
import json
from traceback import print_exc
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from gdsync_functions import update, resolve, get_gtree, get_ltree, trav, get_inc, update_inc, settings

SCOPES = ["https://www.googleapis.com/auth/drive"]
CREDENTIALS = "credentials.json"
TOKEN = "token.json"
treesjson = 'gdtrees.json'
creds = None
sync_delay = 1

if os.path.exists(TOKEN):
    creds = Credentials.from_authorized_user_file(TOKEN, SCOPES)

if not creds or not creds.valid:
    try:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN, "w") as f:
            f.write(creds.to_json())
    except RefreshError as referr:
        if (referr.args[1]['error'] in ('invalid_grant', 'deleted_client')):
            os.remove(TOKEN)
try:
    service = build("drive", "v3", credentials=creds)
    root = service.files().get(fileId='root').execute()['id']   

    while True:
        try: 
            treeset = json.load(open(treesjson))
        except:
            treeset = {}
        try:
            for path, parent, onlycopy in get_inc():
                settings(treeset, path, parent, onlycopy)

            deletions = []
            for path in treeset:
                tree = treeset[path]
                parent = tree.get('fileid')
                onlycopy = tree.get('onlycopy', False)
                print(path, parent, onlycopy)
                            
                #FIXME i brought the below line of updating tree by get_ltree and get_gtree before inclusion path resolve
                update (tree, 
                    update( get_ltree(path),get_gtree(service, parent)
                ))
                if not resolve(service, root, os.path.dirname(path), os.path.basename(path), tree, onlycopy):
                    deletions.append(path)
                    continue
                
                trav(service, path, tree, [], onlycopy)     
                if not update_inc(tree['fileid'], path):
                    deletions.append(path)

            for d in deletions:
                del treeset[d]
            json.dump(treeset, open(treesjson, 'w'), indent=4)
        
        except RefreshError as referr:
            if (referr.args[1]['error'] in ('invalid_grant', 'deleted_client')):
                os.remove(TOKEN)
                break
        except:
            print_exc()
        time.sleep(sync_delay)
except HttpError as e:
    print(e)
except:
    print_exc()