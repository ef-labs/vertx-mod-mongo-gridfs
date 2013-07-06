/*
 * The MIT License (MIT)
 * Copyright © 2013 Englishtown <opensource@englishtown.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the “Software”), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.englishtown;

import com.mongodb.*;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;

/**
 * An EventBus module providing MongoDB GridFS functionality
 */
public class GridFSModule extends Verticle implements Handler<Message<JsonObject>> {

    public static final String DEFAULT_ADDRESS = "et.mongo.gridfs";

    protected EventBus eb;
    protected Logger logger;

    protected String address;
    protected String host;
    protected int port;
    protected String dbName;
    protected String username;
    protected String password;

    protected Mongo mongo;
    protected DB db;

    @Override
    public void start() {
        eb = vertx.eventBus();
        logger = container.logger();

        JsonObject config = container.config();
        address = config.getString("address", DEFAULT_ADDRESS);

        host = config.getString("host", "localhost");
        port = config.getInteger("port", 27017);
        dbName = config.getString("db_name", "default_db");
        username = config.getString("username", null);
        password = config.getString("password", null);
        int poolSize = config.getInteger("pool_size", 10);

        try {
            MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
            builder.connectionsPerHost(poolSize);
            ServerAddress address = new ServerAddress(host, port);
            mongo = new MongoClient(address, builder.build());
            db = mongo.getDB(dbName);
            if (username != null && password != null) {
                db.authenticate(username, password.toCharArray());
            }
        } catch (UnknownHostException e) {
            logger.error("Failed to connect to mongo server", e);
        }

        // Main Message<JsonObject> handler that inspects an "action" field
        eb.registerHandler(address, this);

        // Message<byte[]> handler to save file chunks
        eb.registerHandler(address + "/saveChunk", new Handler<Message<byte[]>>() {
            @Override
            public void handle(Message<byte[]> message) {
                saveChunk(message);
            }
        });

    }

    @Override
    public void stop() {
        mongo.close();
    }

    @Override
    public void handle(Message<JsonObject> message) {

        JsonObject jsonObject = message.body();
        String action = getRequiredString("action", message, jsonObject);
        if (action == null) {
            return;
        }

        try {
            switch (action) {
                case "getFile":
                    getFile(message, jsonObject);
                    break;
                case "getChunk":
                    getChunk(message, jsonObject);
                    break;
                case "saveFile":
                    saveFile(message, jsonObject);
                    break;
                default:
                    sendError(message, "action " + action + " is not supported");
            }

        } catch (Throwable e) {
            sendError(message, "Unexpected error in " + action + ": " + e.getMessage(), e);
        }
    }

    public void saveFile(Message<JsonObject> message, JsonObject jsonObject) {

        ObjectId id = getObjectId(message, jsonObject, "id");
        if (id == null) {
            return;
        }

        int length = jsonObject.getInteger("length", 0);
        if (length <= 0) {
            sendError(message, "length must be greater than zero");
            return;
        }

        int chunkSize = jsonObject.getInteger("chunkSize", 0);
        if (chunkSize <= 0) {
            sendError(message, "chunkSize must be greater than zero");
            return;
        }

        long uploadDate = jsonObject.getLong("uploadDate", 0);
        if (uploadDate <= 0) {
            uploadDate = System.currentTimeMillis();
        }

        String filename = jsonObject.getString("filename");
        String contentType = jsonObject.getString("contentType");

        try {
            BasicDBObjectBuilder builder = BasicDBObjectBuilder.start()
                    .add("_id", id)
                    .add("length", length)
                    .add("chunkSize", chunkSize)
                    .add("uploadDate", new Date(uploadDate));

            if (filename != null) builder.add("filename", filename);
            if (contentType != null) builder.add("contentType", contentType);

            DBObject dbObject = builder.get();

            String bucket = jsonObject.getString("bucket", GridFS.DEFAULT_BUCKET);
            DBCollection collection = db.getCollection(bucket + ".files");

            // Ensure standard indexes as long as collection is small
            if (collection.count() < 1000) {
                collection.ensureIndex(BasicDBObjectBuilder.start().add("filename", 1).add("uploadDate", 1).get());
            }

            collection.save(dbObject);
            sendOK(message);

        } catch (Exception e) {
            sendError(message, "Error saving file", e);
        }
    }

    /**
     * Handler for saving file chunks.
     *
     * @param message The message body is a byte[] where the first four bytes are an int indicating how many bytes are
     *                the json fields, the remaining bytes are the file chunk to write to MongoDB
     */
    public void saveChunk(Message<byte[]> message) {

        JsonObject jsonObject;
        byte[] data;

        // Parse the byte[] message body
        try {
            byte[] body = message.body();

            // First four bytes indicate the json string length
            ByteBuffer byteBuffer = ByteBuffer.wrap(body, 0, 4);
            int len = byteBuffer.getInt();

            // Decode json
            int from = 4;
            byte[] jsonBytes = Arrays.copyOfRange(body, from, from + len);
            String jsonString = decode(jsonBytes);
            jsonObject = new JsonObject(jsonString);

            // Remaining bytes are the chunk to be written
            from += len;
            data = Arrays.copyOfRange(body, from, body.length);

        } catch (RuntimeException e) {
            sendError(message, "error parsing byte[] message.  see the documentation for the correct format", e);
            return;
        }

        // Now save the chunk
        saveChunk(message, jsonObject, data);

    }

    public void saveChunk(Message<byte[]> message, JsonObject jsonObject, byte[] data) {

        if (data == null || data.length == 0) {
            sendError(message, "chunk data is null or empty");
            return;
        }

        ObjectId id = getObjectId(message, jsonObject, "files_id");
        if (id == null) {
            return;
        }

        Integer n = jsonObject.getInteger("n");
        if (n == null) {
            sendError(message, "n (chunk number) is missing");
            return;
        }

        try {
            DBObject dbObject = BasicDBObjectBuilder.start()
                    .add("files_id", id)
                    .add("n", n)
                    .add("data", data).get();

            String bucket = jsonObject.getString("bucket", GridFS.DEFAULT_BUCKET);
            DBCollection collection = db.getCollection(bucket + ".chunks");

            // Ensure standard indexes as long as collection is small
            if (collection.count() < 1000) {
                collection.ensureIndex(
                        BasicDBObjectBuilder.start().add("files_id", 1).add("n", 1).get(),
                        BasicDBObjectBuilder.start().add("unique", 1).get());
            }

            collection.save(dbObject);
            sendOK(message);

        } catch (RuntimeException e) {
            sendError(message, "Error saving chunk", e);
        }

    }

    public void getFile(Message<JsonObject> message, JsonObject jsonObject) {

        ObjectId objectId = getObjectId(message, jsonObject, "id");
        if (objectId == null) {
            return;
        }

        // Optional bucket, default is "fs"
        String bucket = jsonObject.getString("bucket", GridFS.DEFAULT_BUCKET);
        GridFS files = new GridFS(db, bucket);

        GridFSDBFile file = files.findOne(objectId);
        if (file == null) {
            sendError(message, "File does not exist: " + objectId.toString());
            return;
        }

        JsonObject fileInfo = new JsonObject()
                .putString("filename", file.getFilename())
                .putString("contentType", file.getContentType())
                .putNumber("length", file.getLength())
                .putNumber("chunkSize", file.getChunkSize())
                .putNumber("uploadDate", file.getUploadDate().getTime());

        DBObject metaData = file.getMetaData();
        if (metaData != null) {
            fileInfo.putObject("metaData", new JsonObject(JSON.serialize(metaData)));
        }

        // Send file info
        sendOK(message, fileInfo);

    }

    public void getChunk(Message<JsonObject> message, final JsonObject jsonObject) {

        ObjectId id = getObjectId(message, jsonObject, "files_id");

        Integer n = jsonObject.getInteger("n");
        if (n == null) {
            sendError(message, "n (chunk number) is required");
            return;
        }
        if (n < 0) {
            sendError(message, "n must be greater than or equal to zero");
            return;
        }

        String bucket = jsonObject.getString("bucket", GridFS.DEFAULT_BUCKET);

        DBCollection collection = db.getCollection(bucket + ".chunks");
        DBObject dbObject = BasicDBObjectBuilder
                .start("files_id", id)
                .add("n", n)
                .get();

        DBObject result = collection.findOne(dbObject);

        if (result == null) {
            message.reply(new byte[0]);
            return;
        }

        byte[] data = (byte[]) result.get("data");
        boolean reply = jsonObject.getBoolean("reply", false);
        Handler<Message<JsonObject>> replyHandler = null;

        if (reply) {
            replyHandler = new Handler<Message<JsonObject>>() {
                @Override
                public void handle(Message<JsonObject> reply) {
                    int n = jsonObject.getInteger("n") + 1;
                    jsonObject.putNumber("n", n);
                    getChunk(reply, jsonObject);
                }
            };
        }

        message.reply(data, replyHandler);

    }

    public void sendError(Message message, String error) {
        sendError(message, error, null);
    }

    public void sendError(Message message, String error, Throwable e) {
        logger.error(error, e);
        JsonObject result = new JsonObject().putString("status", "error").putString("message", error);
        message.reply(result);
    }

    public void sendOK(Message message) {
        sendOK(message, new JsonObject());
    }

    public void sendOK(Message message, JsonObject response) {
        response.putString("status", "ok");
        message.reply(response);
    }

    private String decode(byte[] bytes) {
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // Should never happen
            throw new RuntimeException(e);
        }
    }

    private ObjectId getObjectId(Message message, JsonObject jsonObject, String fieldName) {

        String idString = getRequiredString(fieldName, message, jsonObject);
        if (idString == null) {
            return null;
        }

        try {
            return new ObjectId(idString);
        } catch (Exception e) {
            sendError(message, fieldName + " " + idString + " is not a valid ObjectId", e);
            return null;
        }

    }

    private String getRequiredString(String fieldName, Message message, JsonObject jsonObject) {
        String value = jsonObject.getString(fieldName);
        if (value == null) {
            sendError(message, fieldName + " is missing");
        }
        return value;
    }

}
