import os
import fnmatch
import datetime
import collections.abc
import time
import mimetypes
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
import shutil
from traceback import print_exc

gdinc = ".gdinc"
gdexc = ".gdexc"
inc_sep = ";"
sep='/'
foldertype = "application/vnd.google-apps.folder"
q_files_folders = "'%s' in parents and trashed = false and (mimeType = 'application/vnd.google-apps.folder' or not mimeType contains 'application/vnd.google-apps.')"
files_fields = "id, name, mimeType, modifiedTime, parents, trashed"

def normpath(path):
    return os.path.normpath(path).replace('\\', sep)
def path_join(path, child):
    return normpath(os.path.join(path, child))

def update_inc(id, path):
    inc = get_inc()
    found = 0
    for i in inc:
        if fnmatch.fnmatch(i[0], path):
            i[1]=id
            found=1
    with open(gdinc, "w") as f:
        f.writelines("\n".join([inc_sep.join(i) for i in inc]))
    return found
def get_inc():
    inc = []
    try:
        if os.path.isfile(gdinc):
            with open(gdinc) as f:
                for line in f.read().splitlines():
                    i = [l.strip() for l in line.split(inc_sep)]
                    if len(i) < 3:
                        continue
                    i[0] = normpath(i[0])
                    if (i[0]!='.'):
                        inc.append(i)
    except:
        print_exc()
    return inc

def list_gfiles(service, parent):
    try:
        return service.files().list(
        q=q_files_folders % parent , fields="files(%s)"%files_fields, 
        spaces='drive', orderBy='modifiedTime').execute().get('files', [])
    except HttpError as err:
        err_is_notFound(err)
        return []
def get_gtree(service, parent):
    files = {}
    folders = {}
    tree = {'files':files, 'folders':folders}

    # walk in gdrive in fileid "parent"
    results = list_gfiles(service, parent)
    if not results:
        return {}
    for gfile in results:
        name = gfile['name']
        fileid = gfile['id']
        if gfile['mimeType'] != foldertype:  
            files[name] = {'fileid': fileid}
        else:
            folder = {'fileid': fileid}
            folders[name] = folder | get_gtree(service, fileid)
    return tree

def get_ltree(path):
    if not os.path.exists(path):
        return {}
    files = {}
    folders = {}
    tree = {'files':files, 'folders':folders}
    # walk in local folders in "path"
    if os.path.isdir(path):
        for name in os.listdir(path):
            fpath = path_join(path, name)
            if os.path.isfile(fpath):
                files[name] = {}
            elif os.path.isdir(fpath):
                folders[name] = get_ltree(fpath)
    return tree

def get_file(service, parent, fileid, filepath):    
    lmodified = gmodified = 0
    if os.path.exists(filepath):
        lmodified = os.path.getmtime(filepath)
    if fileid and parent:        
        try:
            response = service.files().get(fileId=fileid, fields=files_fields).execute()
            if parent in response['parents'] and not response['trashed']:
                gmodified = tsfromz(response['modifiedTime'])              
        except HttpError as err:
            err_is_notFound(err)
    return lmodified, gmodified

def resolve(service, parent, path,name, tree, onlycopy):
    fileid = tree.get('fileid')
    syncts = tree.get('syncts', 0)
    fpath = path_join(path, name)
    lmodified, gmodified = get_file(service, parent, fileid, fpath)
    if lmodified == gmodified == 0:
        return 0
    if syncts and not onlycopy:
        if not lmodified:
            if fileid:
                try:
                    service.files().delete(fileId=fileid).execute()
                except HttpError as err:
                    err_is_notFound(err)
            return 0   
        if not gmodified:
            try:
                if os.path.isfile(fpath):
                    os.remove(fpath)
                elif os.path.isdir(fpath):
                    shutil.rmtree(fpath)
            except:
                print_exc()
            return 0
    
    if not gmodified or lmodified > gmodified and lmodified > syncts:
        if os.path.exists(fpath):
            response = create_or_update(service, parent, fileid, fpath)
            if response and response.get('id'):
                tree['syncts']=time.time()
                tree['fileid'] = response['id']

    elif not lmodified or gmodified > lmodified and gmodified > syncts:
        response = valid_gfile(service, fileid, parent)
        if response and response['mimeType'] == foldertype:
            os.makedirs(fpath, exist_ok=True)
            tree['syncts'] = time.time()
        
        elif download_file(service, fileid, fpath):
            tree['syncts'] = time.time()
    
    return 1

def trav(service, path, tree, exc, onlycopy):
    files = tree.get('files', {})
    folders = tree.get('folders', {})
    parent = tree.get('fileid')
    # for every exclusion in parent exc and local .gdexc and global .gdexc
    exc = [e for e in exc+get_exc(path)+[path_join(path, globalexc) for globalexc in get_exc('')] if is_child(e, path)]
    deletions = []
    for name in files:
        if is_excluded(path_join(path,name), exc) or not resolve(service,parent, path, name, files[name], onlycopy):
            deletions.append(name)
            continue
    for d in deletions:
        del files[d]

    deletions = []
    for name in folders:
        folderpath = path_join(path,name)
        folder = folders[name]
        if is_excluded(folderpath, exc) or not resolve(service,parent, path, name, folder,  onlycopy):
            deletions.append(name)
            continue
        # calling recursively on the folder
        trav(service, folderpath, folder,exc, onlycopy)
    for d in deletions:
        del folders[d]


def settings(treeset, path, id, onlycopy):
    newtree = treeset.get(path, {})
    newtree['fileid'] = id
    newtree['onlycopy'] = onlycopy
    treeset[path] = newtree

def valid_gfile(service, fileid, parent):
    if not fileid:
        return
    response = None
    try:

        response = service.files().get(fileId=fileid, fields=files_fields).execute()
        if parent not in response['parents'] or response['trashed']:
            response = None
    except HttpError as err:
        err_is_notFound(err)
        response = None
    return response

def download_file(service, fileid, filepath):
    try:
        done = False
        with open(filepath, "wb") as file:        
            downloader = MediaIoBaseDownload(file, service.files().get_media(fileId=fileid))        
            while done is False:
                status, done = downloader.next_chunk()
                print(f"Download {int(status.progress() * 100)}% ({filepath})")
        return True
    except HttpError as err:
        return notdownloadable(err)
    except:
        #TODO delete if permission error
        print(filepath, "SOME ERROR IN DOWNLOADING")
        if os.path.isfile(filepath):
            os.remove(filepath)
        elif os.path.isdir(filepath):
            shutil.rmtree(filepath)
        print_exc()

def get_exc(path):
    exc=[]
    gdexc_file = path_join(path, gdexc)
    try:
        if os.path.isfile(gdexc_file):
            with open(gdexc_file) as f:
                for line in f:
                    line = line.strip().strip(sep)
                    if line and not line.startswith("#"):
                        p = path_join(path, line)
                        if p not in exc:
                            exc.append(p)
    except:
        print_exc()
    return exc
def is_child(child, folder):
    common_parts = normpath(os.path.relpath(child, folder)).split(sep)
    return ".." not in common_parts and len(common_parts)>0  and common_parts != ["."]
def is_excluded(path, exc):
    for e in exc:
        if fnmatch.fnmatch(path, e):
            return True
    return False

def create_or_update(service, parent, fileid, f):
    response = None
    name = os.path.basename(f)
    mimetype = foldertype if os.path.isdir(f) else mimetypes.guess_type(f)[0]
    metadata = {'name':name, 'mimeType':mimetype}
    try:
        media = MediaFileUpload(f, resumable=True) if mimetype != foldertype else None
        upload_request = None
        if valid_gfile(service, fileid, parent):
            upload_request = service.files().update(
                    fileId=fileid,body=metadata,media_body=media,fields=files_fields,
            )
        elif parent:
            metadata["parents"] = [parent]
            upload_request = service.files().create(
                body=metadata, media_body=media, fields=files_fields)
            
        
        if upload_request:
            if media:
                while not response:
                    status, response = upload_request.next_chunk()
                    if status:
                        print("Uploaded %d%% (%s)" % (int(status.progress() * 100), f))
            else:
                response = upload_request.execute()
    except HttpError as err:
        err_is_notFound(err)
    except:
        print_exc()
    return response

def err_is_notFound(err):
    if hasattr(err, "error_details") and any(errdetail["reason"] == "notFound"
        for errdetail in err.error_details if "reason" in errdetail
    ):
        print("NOT FOUND")
        
def notdownloadable(err):
    if any(errdetail["reason"] == "fileNotDownloadable" for errdetail in err.error_details):
        print("NOT DOWNLOADABLE")

def tsfromz(ztime):
    return datetime.datetime.strptime(ztime, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc).timestamp()

def update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = update(d.get(k, {}), v)
        else:
            d[k] = v
    return d
