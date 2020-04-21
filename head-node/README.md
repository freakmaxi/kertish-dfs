# Kertish DFS Head Node

Head node is responsible to handle file system client and dfs using manager node and data nodes.
Default bind endpoint port is `:4000`

Head node keep the metadata of files/folders in mongo db and metadata stability is supported
with Redis dss.

Should be started with parameters that are set as environment variables

### Environment Variables
- `BIND_ADDRESS` (optional) : Service binding address. Ex: `127.0.0.1:4000` Default: `:4000`

Client will access the service using `http://127.0.0.1:4000/client/dfs`

- `MANAGER_ADDRESS` (mandatory) : Manager Node accessing endpoint. Ex: `http://127.0.0.1:9400`

Manager address will be used to create the data node mapping, reservation, discard and commit 
operations of file/folder placement.

- `MONGO_CONN` (mandatory) : Mongo DB endpoint. Ex: `mongodb://admin:password@127.0.0.1:27017`

Metadata of the file system will be kept in Mongo DB.

- `MONGO_DATABASE` (optional) : Mongo DB name. Default: `kertish-dfs`

- `REDIS_CONN` (mandatory) : Redis dss. Ex: `127.0.0.1:6379`

Will be used to have the stability of metadata of the file system

- `REDIS_CLUSTER_MODE` : Redis cluster mode activation Ex: `true`

### File System Manipulation Requests

- `GET` is used to get folders/files list and also file downloading.

##### Required Headers:
- `X-Path` folder(s)/file(s) location in dfs. Possible formats are `[sourcePath]` or to join files 
`j,[sourcePath],[sourcePath]...`. `sourcePath`(s) should be url encoded

##### Optional Headers:
- `X-Calculate-Usage` (only folder) force to calculate the size of folders
- `X-Download` works only with file request. It provides the data with `Content-Disposition` header. Values: `1` or `true`. Default: `false`
- `Range` to grab the part of the file. 

##### Possible Responses
- `X-Type` (always) : give the information about the content. Value: `file` or `folder`  
- `Accept-Ranges` (only file)
- `Content-Length` (only file)
- `Content-Type` (only file)
- `Content-Disposition` (only file request with download flag) 
- `Content-Encoding` (only file request with range header)
- `Content-Range` (only file request with range header)

##### Possible Status Codes
- `404`: Not found
- `416`: Range dissatisfaction
- `422`: Required Request Headers are not valid or absent
- `500`: Operational failures
- `200`: Successful

##### Folder Sample Response
```json
{
  "full": "/",
  "name": "",
  "created": "2020-01-11T21:15:55.23Z",
  "modified": "2020-01-11T21:15:55.23Z",
  "folders": [
    {
      "full": "/FolderName",
      "name": "FolderName",
      "created": "2020-01-13T13:13:22.243Z",
      "size": 0
    }
  ],
  "files": [
    {
      "name": "contacts.csv",
      "mime": "text/plain; charset=utf-8",
      "size": 2231,
      "created": "2020-01-13T13:14:11.627Z",
      "modified": "2020-01-13T13:14:11.627Z",
      "chunks": [
        {
          "sequence": 0,
          "size": 2231,
          "hash": "e5c0adae0f05cf60f7e34b45bd44249f42627b1f3b1b453ae45e106adbfdfbdb"
        }
      ],
      "locked": false
    }
  ],
  "size": 0
}
```
---
- `POST` is used to create folders and upload files.

##### Required Headers:
- `X-Apply-To` is the aim of operation. Values: `file` or `folder`
- `X-Path` folder/file location in dfs (should be urlencoded)
- `Content-Type` (only file)
- `Content-Length` (only file)

##### Optional Headers:
- `X-Allow-Empty` (only file) allow zero length file upload. Values: `1` or `true`. Default: `false`
- `X-Overwrite` (only file) ignore file existence and continue without conflict response. Values: `1` or `true`. Default: `false` 

##### Possible Status Codes
- `409`: Conflict (folder/file exists)
- `411`: Content Length is required
- `422`: Required Request Headers are not valid or absent
- `500`: Operational failures
- `507`: Out of disk space
- `202`: Accepted
---
- `PUT` is used to move/copy folders/files in file system.

##### Required Headers:
- `X-Path` source folder(s)/file(s) location in dfs. Possible formats are `[sourcePath]` or for file/folder joining
`j,[sourcePath],[sourcePath]...`. `sourcePath`(s) should be url encoded
- `X-Target` action and target of folder/file. it is formatted header, the value must be `[action],[targetPath]` and
`targetPath` should be url encoded. 
`c` is used for copy action, `m` is used for move action. Ex: `c,/SomeTargetFolder` or `m,/SomeTargetFolder` 
- `X-Overwrite` ignore file/folder existence and continue without conflict response. Values: `1` or `true`. Default: `false`

##### Possible Status Codes
- `404`: Source not found
- `409`: Conflict (folder/file exists)
- `412`: Conflict when joining folders
- `422`: Required Request Headers are not valid or absent
- `500`: Operational failures
- `200`: Accepted
---
- `DELETE` is used to delete folders/files in file system.
**CAUTION: Deletion operation is applied immediately**

##### Required Headers:
- `X-Path` source folder/file location in dfs (should be urlencoded)
- `X-Kill-Zombies` force zombie file/folder to be removed. Values: `1` or `true`. Default: `false`

##### Possible Status Codes
- `404`: Not found
- `422`: Required Request Headers are not valid or absent
- `500`: Operational failures
- `524`: Zombie file or folder has zombie file(s)
- `525`: Zombie file or folder is still alive, try again to kill
- `200`: Accepted

