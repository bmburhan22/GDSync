import os
import fnmatch
import datetime
import collections.abc
import time
import mimetypes
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import shutil
from logging import getLogger, Formatter, FileHandler, StreamHandler, INFO
import json

def tsfromz(ztime):
    return datetime.datetime.strptime(ztime, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc).timestamp()

def update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = update(d.get(k, {}), v)
        else:
            d[k] = v
    return d

def is_excluded(path, exc):
    for e in exc:
        if fnmatch.fnmatch(path, e):
            return True
    return False

class GDSync:
    service=None
    creds = None
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    CREDENTIALS = "credentials.json"
    TOKEN = "token.json"
    SYNC_DELETIONS_KEYWORD = 'syncdel'
    gdinc = ".gdinc"
    gdexc = ".gdexc"
    inc = []
    treesjson = 'gdtrees.json'
    inc_sep = ";"
    sep='/'
    foldertype = "application/vnd.google-apps.folder"
    q_files_folders = "'me' in owners and trashed = false and (mimeType = 'application/vnd.google-apps.folder' or not mimeType contains 'application/vnd.google-apps.')"
    files_fields = "id, name, mimeType, modifiedTime, parents, trashed"
    logger = None
    root = 'root'
    treeset = {}
    gfiles = {}
    
    def __init__(self, root='root'):
        self.logger = getLogger('gdsynclogger')
        self.logger.setLevel(INFO)
        formatter = Formatter('%(asctime)s\t\t%(message)s')
        handler = FileHandler('info.log') 
        handler.setFormatter(formatter)
        console = StreamHandler()
        console.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.addHandler(console)

        self.get_treeset()
        self.get_creds()
        self.service = build("drive", "v3", credentials=self.creds)
        self.root = self.get_root(root)
        self.get_gfiles()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        return 

    def get_creds(self):
        if os.path.exists(self.TOKEN):
            self.creds = Credentials.from_authorized_user_file(self.TOKEN,self.SCOPES)

        if not self.creds or not self.creds.valid:
            try:
                if self.creds and self.creds.expired and self.creds.refresh_token:
                    self.creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(self.CREDENTIALS, self.SCOPES)
                    self.creds = flow.run_local_server(port=0)
                with open(self.TOKEN, "w") as f:
                    f.write(self.creds.to_json())
            except RefreshError as referr:
                if (referr.args[1]['error'] in ('invalid_grant', 'deleted_client')):
                    os.remove(self.TOKEN)           

    def get_root(self, root):
        try:
            return self.service.files().get(fileId=root).execute()['id']
        except:
            return self.get_root('root')
    
    def update_inc(self, fileid, path):
        self.get_inc()
        for i in self.inc:
            if fnmatch.fnmatch(i[0], path):
                i[1]=fileid
                break
        with open(self.gdinc, "w") as f:
            f.writelines("\n".join([self.inc_sep.join(i) for i in self.inc]))

    def get_inc(self):
        try:
            self.inc=[]
            if os.path.isfile(self.gdinc):
                with open(self.gdinc) as f:
                    for line in f.read().splitlines():
                        i = [l.strip() for l in line.split(self.inc_sep)]
                        if len(i) < 3:
                            continue
                        i[0] = self.normpath(i[0])
                        if (i[0]!='.'):
                            self.inc.append(i)
        except Exception as exception:
            self.logger.info(exception)
    
    def get_treeset(self):
        try: 
            old_treeset = json.load(open(self.treesjson))
        except:
            old_treeset = {}
        self.treeset = {}    
        self.get_inc()
        for path, parent, syncdel in self.inc:
            tree = old_treeset.get(path, {})
            tree['fileid'] = parent
            tree['syncdel'] = syncdel
            self.treeset[path] = tree

    def get_gfiles(self):
        self.gfiles = {}
        page_token = None
        while True:
            try:
                response = self.service.files().list(q=self.q_files_folders,
                    fields="nextPageToken, files(%s)"%self.files_fields,
                    spaces='drive', orderBy='modifiedTime',
                    pageToken=page_token,pageSize=1000
                ).execute()

                self.gfiles.update({gfile['id']:gfile for gfile in response.get('files', [])})
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
                print("Fetched %s files"%len(self.gfiles))
            except HttpError as err:
                self.logger.info('Could not list google drive files=;HTTP Error %s'% err)
            except Exception as exception:
                self.logger.info("%s" %exception)

    def get_gtree(self, parent):
        tree = {'fileid':parent, 'files':{}, 'folders':{}}
        for fileid, gfile in self.gfiles.items():
            if parent in gfile.get('parents', []):
                name = gfile['name']
                if gfile['mimeType'] != self.foldertype:  
                    tree['files'][name] = {'fileid': fileid}
                else:
                    tree['folders'][name] = self.get_gtree(fileid)
        return tree
    
    def get_ltree(self, path):
        if not os.path.exists(path):
            return {}
        files = {}
        folders = {}
        tree = {'files':files, 'folders':folders}
        # walk in local folders in "path"
        if os.path.isdir(path):
            for name in os.listdir(path):
                fpath = self.path_join(path, name)
                if os.path.isfile(fpath):
                    files[name] = {}
                elif os.path.isdir(fpath):
                    folders[name] = self.get_ltree(fpath)
        return tree
    
    def get_file(self, parent, fileid, filepath):    
        lmodified = gmodified = 0
        if os.path.exists(filepath):
            lmodified = os.path.getmtime(filepath)
        if fileid and parent:        
            try:
                gfile = self.gfiles[fileid]

                if parent in gfile['parents'] and not gfile['trashed']:
                    gmodified = tsfromz(gfile['modifiedTime'])     

            except Exception as exception:
                self.logger.info(exception)
        return lmodified, gmodified

    def get_exc(self, path):
        exc=[]
        gdexc_file = self.path_join(path, self.gdexc)
        try:
            if os.path.isfile(gdexc_file):
                with open(gdexc_file) as f:
                    for line in f:
                        line = line.strip().strip(self.sep)
                        if line and not line.startswith("#"):
                            p = self.path_join(path, line)
                            if p not in exc:
                                exc.append(p)
        except Exception as exception:
            self.logger.info(exception)
        return exc
    
    def path_join(self, path, child):
        return self.normpath(os.path.join(path, child))

    def normpath(self, path):
        return os.path.normpath(path).replace('\\', self.sep)

    def is_child(self, child, folder):
        common_parts = self.normpath(os.path.relpath(child, folder)).split(self.sep)
        return ".." not in common_parts and len(common_parts)>0  and common_parts != ["."]

    def valid_gfile(self, fileid, parent):
        if not fileid:
            return
        response = None
        try:
            response = self.service.files().get(fileId=fileid, fields=self.files_fields).execute()
            if parent not in response['parents'] or response['trashed']:
                self.logger.info('Google drive fileid=%s not present in parent=%s'%(fileid,parent))
                response = None
        except HttpError as err:
            response = None
            self.logger.info('Could not find google drive fileid=%s in parent=%s;HTTP Error %s'%(fileid,parent, err))
        return response

    def download_file(self, fileid, filepath):
        try:
            self.logger.info("Downloading;%s;%s"%(fileid, filepath))
            done = False
            with open(filepath, "wb") as file:        
                downloader = MediaIoBaseDownload(file, self.service.files().get_media(fileId=fileid))        
                while done is False:
                    status, done = downloader.next_chunk()
                    print(f"Downloaded {int(status.progress() * 100)}% ({filepath})")
            return True
        except HttpError as err:
            self.logger.info('Could not download google drive fileid=%s;HTTP Error %s'%(fileid, err))
        except Exception as exception:
            #TODO delete if permission error
            self.logger.info("SOME ERROR IN DOWNLOADING %s to local file %s"%(fileid, filepath))
            self.logger.info(exception)
            if os.path.isfile(filepath):
                os.remove(filepath)
            elif os.path.isdir(filepath):
                shutil.rmtree(filepath)

    def create_or_update(self, parent, fileid, f):
        self.logger.info("Uploading;%s;%s"%(fileid, f))
        response = None
        name = os.path.basename(f)
        mimetype = self.foldertype if os.path.isdir(f) else mimetypes.guess_type(f)[0]
        metadata = {'name':name, 'mimeType':mimetype}
        try:
            media = MediaFileUpload(f, resumable=True) if mimetype != self.foldertype else None
            upload_request = None
            if self.valid_gfile( fileid, parent):
                upload_request = self.service.files().update(
                        fileId=fileid,body=metadata,media_body=media,fields=self.files_fields,
                )
            elif parent:
                metadata["parents"] = [parent]
                upload_request = self.service.files().create(
                    body=metadata, media_body=media, fields=self.files_fields)
                
            
            if upload_request:
                if media:
                    while not response:
                        status, response = upload_request.next_chunk()
                        if status:
                            print(f"Uploaded {int(status.progress() * 100)}% ({f})")

                else:
                    response = upload_request.execute()
        except HttpError as err:
            self.logger.info('Could not create google drive fileid=%s;HTTP Error %s'%(fileid, err))
        except Exception as exception:
            self.logger.info(exception)
        return response

    def resolve(self, parent, path,name, tree, syncdel):
        fileid = tree.get('fileid')
        syncts = tree.get('syncts', 0)
        fpath = self.path_join(path, name)
        lmodified, gmodified = self.get_file(parent, fileid, fpath)
        if lmodified == gmodified == 0:
            return 0
        if syncts and syncdel==self.SYNC_DELETIONS_KEYWORD:
            if not lmodified:
                if fileid:
                    try:
                        self.service.files().delete(fileId=fileid).execute()
                    except HttpError as err:
                        self.logger.info('Could not delete google drive fileid=%s;HTTP Error %s'%(fileid, err))
                return 0   
            if not gmodified:
                try:
                    if os.path.isfile(fpath):
                        os.remove(fpath)
                    elif os.path.isdir(fpath):
                        shutil.rmtree(fpath)
                except Exception as exception:
                    self.logger.info(exception)
                return 0
        
        if not gmodified or lmodified > gmodified and lmodified > syncts:
            if os.path.exists(fpath):
                response = self.create_or_update(parent, fileid, fpath)
                if response and response.get('id'):
                    tree['syncts']=time.time()
                    tree['fileid'] = response['id']
                    self.logger.info("Uploaded;%s;%s"%(tree['fileid'], fpath))


        elif not lmodified or gmodified > lmodified and gmodified > syncts:
            response = self.valid_gfile(fileid, parent)
            if response and response['mimeType'] == self.foldertype:
                os.makedirs(fpath, exist_ok=True)
                tree['syncts'] = time.time()
            
            elif self.download_file(fileid, fpath):
                tree['syncts'] = time.time()
                self.logger.info("Downloaded;%s;%s"%( fileid, fpath))
        
        return 1

    def trav(self, path, tree, exc, syncdel):
        files = tree.get('files', {})
        folders = tree.get('folders', {})
        parent = tree.get('fileid')
        # for every exclusion in parent exc and local .gdexc and global .gdexc
        exc = [e for e in exc+self.get_exc(path)+[self.path_join(path, globalexc) for globalexc in self.get_exc('')] if self.is_child(e, path)]
        deletions = []
        for name in files:
            if is_excluded(self.path_join(path,name), exc) or not self.resolve(parent, path, name, files[name], syncdel):
                deletions.append(name)
                continue
        for d in deletions:
            del files[d]

        deletions = []
        for name in folders:
            folderpath = self.path_join(path,name)
            folder = folders[name]
            if is_excluded(folderpath, exc) or not self.resolve(parent, path, name, folder,  syncdel):
                deletions.append(name)
                continue
            # calling recursively on the folder
            self.trav(folderpath, folder, exc, syncdel)
        for d in deletions:
            del folders[d]
    
    def run(self):  
        try:    
            deletions = []
            for path in self.treeset:
                tree = self.treeset[path]
                name = os.path.basename(path)
                syncdel =  tree.get('syncdel')
                parent = tree.get('fileid')
                self.logger.info("Inclusion %s;%s;%s"%(path, parent, syncdel))
                parent = self.gfiles.get(parent) and self.root in self.gfiles.get(parent, dict()).get('parents') and parent or next(
                                (fileid for fileid, gfile in self.gfiles.items() 
                                    if gfile.get('name')==name and self.root in gfile.get('parents', [])
                                ), None)                          
                #FIXME i brought the below line of updating tree by get_ltree and get_gtree before inclusion path resolve
                update (tree, 
                    update( self.get_ltree(path), self.get_gtree(parent))
                )
                if not self.resolve(self.root, os.path.dirname(path), os.path.basename(path), tree, syncdel):
                    deletions.append(path)
                    continue
                self.trav(path, tree, [], syncdel)     
                self.update_inc(tree['fileid'], path)
            for d in deletions:
                del self.treeset[d]
            json.dump(self.treeset, open(self.treesjson, 'w'), indent=4)
        except RefreshError as referr:
            if (referr.args[1]['error'] in ('invalid_grant', 'deleted_client')):
                os.remove(self.TOKEN)
            self.logger.info('Refresh Error %s'%(referr))
        except HttpError as err:
            self.logger.info('Could not run;HTTP Error %s'%(err))
        except Exception as exception:
            self.logger.info(exception)
