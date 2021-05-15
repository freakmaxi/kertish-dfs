# Kertish DFS Head Node (DFS)

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

- `HOOKS_PATH` (optional) : The path of hook provider plugins. Default: `./hooks`
  
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
  "size": 0,
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
  "hooks": [
    {
      "id": "c7905a8e6fab03fa3643d81fce611d56",
      "created": "2021-05-15T11:28:38.524Z",
      "runOn": 1,
      "recursive": true,
      "provider": "rabbitmq",
      "setup": {
        "connectionUrl": "amqp://admin:admin@rabbitmq.server.com:5672/",
        "targetQueueTopic": "testQueueName"
      }
    }
  ]
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

##### Body
- `Binary data` (only file)

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

# Kertish DFS Head Node (HOOKS)

Hooks can be considered as watchers for the specific folder. They are executed on some
certain conditions. Kertish DFS supports custom made hook providers. You can find more
information under - [hook-providers](https://github.com/freakmaxi/kertish-dfs/tree/master/hook-providers) path.

The management of the hook registration is handled by the head node.

### Hook Manipulation Requests

- `GET` is used to get the available hook providers registered in the head node.

##### Available Hook Providers Sample Response
```json
[
  {
    "provider": "rabbitmq",
    "version": "21.2.0084-302863",
    "sample": {
      "connectionUrl": "amqp://test:test@127.0.0.1:5672/",
      "targetQueueTopic": "testQueueName"
    }
  }
]
```
---
- `POST` is used to register a hook to a folder or folders.

##### Required Headers:
- `X-Path` folder(s) location in dfs. Possible formats are `[folderPath]` or for multiple folders
  `[folderPath],[folderPath]...`. `folderPath`(s) should be url encoded

##### Body
- `Hook Implementation JSON`

Hook is structured base on the provider that you want to use as hook. You can take
`sample` field as the hook setup.

You can register only one hook to a folder or multiple folders. If you want to register a different
hook, you should make this call with the new hook setup body.


##### Example Hook Registration Body
```json
{
  "runOn": 1,
  "recursive": true,
  "provider": "rabbitmq",
  "setup": {
    "connectionUrl": "amqp://admin:admin@rabbitmq.server.com:5672/",
    "targetQueueTopic": "testQueueName"
  }
}
```

- `runOn` is the case of hook execution. Possible values are, `1` executes hook on any change,
  `2` executes hook on only file or folder is created, 
  `3` executes hook on only file or folder is updated, such as moved or copied, 
  `4` executes hook on only file or folder is deleted
- `recursive` is about tracking the changes under the folder tree. So, if you add the hook
  to a parent folder with `recursive` as `true`, this will be trigger on changes that happen
  on any sub folder(s) of this parent folder.
- `provider` is the provider id to make the hook execution relation. (`provider` field in available providers list)
- `setup` is the provider setup and this field can change base on the hook provider setup needs.
Each provider has its own setup procedure before to take action. (`sample` field in available providers list)
  
##### Important Note
Hooks are executed in a sync manner. That means it will be executed after the dfs operation and
will wait until the hook finishes the execution. If you are adding hooks which do not have any
lazy execution implementation this will slow down the file operation and decrease the performance
of the DFS.

You can add a hook to a parent and children. If there will be more than one hook in the chain to
execute in the folder tree, this will be done from the children hooks to parent hooks direction.
So, when you make the changes in a sub folder with a hook, that hook will be executed before the 
parent hook execution. This execution will be serial and each execution will wait the other one
finished before starting the next execution.

##### Possible Sample Response
```json
[
  "6a28b95cb2338d57028c0a72eb8a54c9"
]
```

If the hook registration is completed successfully, system will create a hook id for this request. The 
created hook id will return as the string array. Having an array does not mean that there can be
multiple values in the array. There will always be a single value.

##### Possible Status Codes
- `422`: Required Request Headers are not valid or absent
- `500`: Operational failures
- `202`: Accepted
---
- `DELETE` is used to delete/unregister hook(s) from the folder.

##### Required Headers:
- `X-Path` folder location in dfs (should be urlencoded)

##### Body
- `HookId Array`

##### Example Hook Deletion Body
```json
[
  "6a28b95cb2338d57028c0a72eb8a54c9",
  "028c0a72eb8a54c96a28b95cb2338d57"
]
```

`HookId` can be taken from the folder details. Folder has `hooks` field that exposes the hook
registration. Every hook will have an `id` after the hook registration.

##### Possible Status Codes
- `404`: Folder not found
- `422`: Required Request Headers are not valid or absent
- `500`: Operational failures
- `200`: Successful