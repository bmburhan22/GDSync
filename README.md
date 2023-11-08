# GDSync

Sync files between Google Drive and desktop. Backup from desktop to Google Drive and the other way around. Any changes on one side is mirrored to the other.

Uses Google (Drive) API OAuth Client credentials in credentials.json

## `.gdinc` file 
Holds lines of `path/to/backup;google_drive_fileid;copyonly` semi-colon (;) separated 
>**NOTE:** If copyonly is empty or whitespace, files are in sync mode, with deletions on one side reflecting on the other side. Any non-whitespace character in copyonly indicates only to copy from one side having the file to other side that does not have the file and vice-versa.

## `.gdexc` file

Inside the program folder holds **global** exclusions

Inside any other backup folder holds **local** recursive exclusions 