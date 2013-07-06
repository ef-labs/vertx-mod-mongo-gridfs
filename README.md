# MongoDB GridFS

Provides MongoDB GridFS support via the Vert.x event bus keeping file chunks in a binary format.  See the MongoDB site for more information on GridFS:
http://docs.mongodb.org/manual/reference/gridfs/


## Dependencies

This module requires a MongoDB server to be available on the network.


## Configuration

The MongoDB GridFS module takes the following configuration:

    {
        "address": <address>,
        "host": <host>,
        "port": <port>,
        "db_name": <db_name>,
        "pool_size": <pool_size>
    }

For example:

    {
        "address": "test.my_persistor",
        "host": "192.168.1.100",
        "port": 27000,
        "pool_size": 20,
        "db_name": "my_db"
    }

Let's take a look at each field in turn:

* `address` The main address for the module. Every module has a main address. Defaults to `et.mongo.gridfs`.
* `host` Host name or ip address of the MongoDB instance. Defaults to `localhost`.
* `port` Port at which the MongoDB instance is listening. Defaults to `27017`.
* `db_name` Name of the database in the MongoDB instance to use. Defaults to `default_db`.
* `pool_size` The number of socket connections the module instance should maintain to the MongoDB server. Default is 10.


## Operations

The module supports the following operations

### Get File

Returns GridFS file information for a given id.

Send a JSON message to the module main address:

    {
        "action": "getFile",
        "id": <id>,
        "bucket": <bucket>
    }

Where:
* `id` is the ObjectId of the GridFS file. This field is mandatory.
* `bucket` is GridFS bucket the file was saved under.  The default value is "fs".

An example would be:

    {
        "action": "getFile",
        "id": "51d864754728011036adc575",
        "bucket": "my_bucket"
    }

When the getFile completes successfully, a reply message is sent back to the sender with the following data:

    {
        "status": "ok",
        "filename": <filename>,
        "contentType": <contentType>,
        "length": <length>,
        "chunkSize": <chunkSize>,
        "uploadDate": <uploadDate>,
        "metaData": <metaData>
    }

Where:
* `filename` is the filename provided when saving
* `contentType` is the content type (ex. image/jpeg)
* `length` is the total file length in bytes
* `chunkSize` is the size in bytes of each chunk
* `uploadDate` is the long time of the upload in milliseconds since 1 Jan 1970
* `metaData` is an optional json object with additional meta data

If an error occurs in saving the document a reply is returned:

    {
        "status": "error",
        "message": <message>
    }

Where `message` is an error message.


### Get Chunk

Returns a GridFS file chunk

Send a JSON message to the module main address:

    {
        "action": "getChunk",
        "files_id": <files_id>,
        "n": <n>,
        "bucket": <bucket>,
        "reply": <reply>
    }

Where:
* `files_id` is the ObjectId of the file
* `n` is the chunk number (first chunk is 0).
* `bucket` is GridFS bucket the file was saved under.  The default value is "fs".
* `reply` is a boolean flag indicating a reply message handler should be added to send the next chunk


An example would be:

    {
        "action": "getChunk",
        "files_id": "51d864754728011036adc575",
        "n": 0,
        "bucket": "my_bucket",
        "reply": true
    }

When the get chunk completes successfully, a reply message with the chunk data byte[] in the message body is returned.

If an error occurs when getting the chunk, a json message is returned:

    {
        "status": "error",
        "message": <message>
    }

Where `message` is an error message.


### Save File

Saves the file information.

Send a JSON message to the module main address:

    {
        "action": "saveFile",
        "id": <id>,
        "length": <length>,
        "chunkSize": <chunkSize>,
        "uploadDate": <uploadDate>,
        "filename": <filename>,
        "contentType": <contentType>,
        "bucket": <bucket>
    }

Where:
* `id` is the ObjectId of the file.
* `length` is the total file length in bytes
* `chunkSize` is the size in bytes of each chunk
* `uploadDate` is the long time of the upload in milliseconds since 1 Jan 1970.  The field is optional.
* `filename` is the filename provided when saving.  This field is optional.
* `contentType` is the content type (ex. image/jpeg).  This field is optional (but recommended).
* `bucket` is GridFS bucket the file was saved under.  The default value is "fs".

An example would be:

    {
        "action": "saveFile",
        "id": "51d864754728011036adc575",
        "length": 161966,
        "chunkSize": 102400,
        "contentType": "image/jpeg"
    }

When the save completes successfully, a reply message is sent back to the sender with the following data:

    {
        "status": "ok"
    }

If an error occurs when saving the file information a reply is returned:

    {
        "status": "error",
        "message": <message>
    }

Where `message` is an error message.


### saveChunk

Saves a chunk of binary data in the GridFS format.

Send a byte[] message to the module main address + "/saveChunk".  The byte[] is made up of 3 parts.

The first four bytes are an int defining the length of a UTF-8 encoded json string.  The json bytes are next and the remaining bytes are the chunk to be saved.

The json contains the following fields:

    {
        "files_id": <files_id>,
        "n": <n>,
        "bucket": <bucket>
    }

Where:
* `files_id` is the ObjectId of the file
* `n` is the chunk number (first chunk is 0).
* `bucket` is GridFS bucket the file was saved under.  The default value is "fs".


When the save completes successfully, a reply message is sent back to the sender with the following data:

    {
         "status": "ok"
    }
