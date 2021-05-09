# Kertish DFS Head Node

Head node is responsible to handle file storage manipulation requests.
Default bind endpoint port is `:4000`

Head node keep the metadata of files/folders in mongo db and metadata stability is supported with Locking-Center

Should be started with parameters that are set as environment variables

### Environment Variables
- `BIND_ADDRESS` (optional) : Service binding address. Ex: `127.0.0.1:4000` Default: `:4000`

Client will access the service using `http://127.0.0.1:4000/client/dfs`

- `MANAGER_ADDRESS` (mandatory) : Manager Node accessing endpoint. Ex: `http://127.0.0.1:9400`

Manager address will be used to create the data node mapping, reserve, discard and commit 
operations of file/folder placement.

- `MONGO_CONN` (mandatory) : Mongo DB endpoint. Ex: `mongodb://admin:password@127.0.0.1:27017`

Metadata of the file storage will be kept in Mongo DB.

- `MONGO_DATABASE` (optional) : Mongo DB name. Default: `kertish-dfs`

- `MONGO_TRANSACTION` (optional) : Set `true` if you have a Mongo DB Cluster setup 

- `LOCKING_CENTER` (mandatory) : Locking-Center Server. Ex: `127.0.0.1:22119`

Will be used to have the stability of metadata of the file storage

### File Storage Manipulation Requests

- `GET` is used to get folders/files list and also file downloading.

##### Required Headers:
- `X-Path` folder(s)/file(s) location in dfs. Possible formats are `[sourcePath]` or to join files 
`j,[sourcePath],[sourcePath]...`. `sourcePath`(s) should be url encoded

##### Optional Headers:
- `X-Calculate-Usage` (only folder) force to calculate the size of folders. Values: `1` or `true`. Default: `false`
- `X-Tree` (only folder) export folder tree. Values: `1` or `true`. Default: `false`
- `X-Download` works only with file request. It provides the data with `Content-Disposition` header. Values: `1` or 
`true`. Default: `false`
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
- `503`: Not available for reservation (Frozen or Paralysed cluster/node)
- `523`: File or folder has lock
- `524`: Zombie file or folder has zombie file(s)
- `200`: Successful
- `206`: Partial Content

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
      "lock": {
        "till": "2020-01-13T13:14:11.627Z"
      }       
    }
  ],
  "size": 0
}
```

##### Folder Tree Sample Response
```json
{
  "full": "/",
  "name": "",
  "created": "2020-04-21T19:18:18.136Z",
  "modified": "2020-07-07T19:48:41.441Z",
  "size": 0,
  "folders": [
    {
      "full": "/FolderName",
      "name": "FolderName",
      "created": "2020-04-23T08:58:09.757Z",
      "modified": "2020-07-06T11:38:02.01Z",
      "size": 0,
      "folders": [
        {
          "full": "/FolderName/SubFolderName1",
          "name": "SubFolderName1",
          "created": "2020-04-23T08:58:09.758Z",
          "modified": "2020-04-23T08:58:09.758Z",
          "size": 0,
          "folders": []
        },
        {
          "full": "/FolderName/SubFolderName2",
          "name": "SubFolderName2",
          "created": "2020-04-23T09:57:17.87Z",
          "modified": "2020-04-23T09:57:17.87Z",
          "size": 0,
          "folders": []
        }
      ]
    }
  ]
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
- `X-Overwrite` (only file) ignore file existence and continue without conflict response. Values: `1` or `true`. 
Default: `false` 

##### Possible Status Codes
- `409`: Conflict (folder/file exists)
- `411`: Content Length is required
- `422`: Required Request Headers are not valid or absent
- `500`: Operational failures
- `503`: Not available for reservation (Frozen or Paralysed cluster/node)
- `507`: Out of disk space
- `202`: Accepted
---
- `PUT` is used to move/copy folders/files in file storage.

##### Required Headers:
- `X-Path` source folder(s)/file(s) location in dfs. Possible formats are `[sourcePath]` or for file/folder joining
`j,[sourcePath],[sourcePath]...`. `sourcePath`(s) should be url encoded
- `X-Target` action and target of folder/file. it is formatted header, the value must be `[action],[targetPath]` and
`targetPath` should be url encoded. 
`c` is used for copy action, `m` is used for move action. Ex: `c,/SomeTargetFolder` or `m,/SomeTargetFolder` 
- `X-Overwrite` ignore file/folder existence and continue without conflict response. Values: `1` or `true`. Default: 
`false`

##### Possible Status Codes
- `404`: Source not found
- `406`: Not Acceptable (folder is not empty)
- `409`: Conflict (folder/file exists)
- `412`: Conflict when joining folders
- `422`: Required Request Headers are not valid or absent
- `500`: Operational failures
- `503`: Not available for reservation (Frozen or Paralysed cluster/node)
- `524`: Zombie file or folder has zombie file(s)
- `200`: Successful
---
- `DELETE` is used to delete folders/files in file storage.
**CAUTION: Deletion operation is applied immediately**

##### Required Headers:
- `X-Path` source folder/file location in dfs (should be urlencoded)
- `X-Kill-Zombies` force zombie file/folder to be removed. Values: `1` or `true`. Default: `false`

##### Possible Status Codes
- `404`: Not found
- `422`: Required Request Headers are not valid or absent
- `500`: Operational failures
- `503`: Not available for reservation (Frozen or Paralysed cluster/node)
- `523`: File or folder has lock
- `524`: Zombie file or folder has zombie file(s)
- `525`: Zombie file or folder is still alive, try again to kill
- `526`: Require consistency repair
- `200`: Successful

