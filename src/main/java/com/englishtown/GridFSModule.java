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
public class GridFSModule extends Verticle {

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

        eb.registerHandler(address + "/getMetaData", new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                getMetaData(message);
            }
        });

        eb.registerHandler(address + "/getChunk", new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                getChunk(message);
            }
        });

        eb.registerHandler(address + "/saveChunk", new Handler<Message<byte[]>>() {
            @Override
            public void handle(Message<byte[]> message) {
                saveChunk(message);
            }
        });

        eb.registerHandler(address + "/saveFile", new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                saveFile(message);
            }
        });

    }

    @Override
    public void stop() {
        mongo.close();
    }

    public void saveFile(Message<JsonObject> message) {

        JsonObject jsonObject = message.body();

        String idString = jsonObject.getString("id");
        if (idString == null) {
            sendError(message, "id is missing");
            return;
        }
        ObjectId id = getObjectId(message, idString);
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

    public void saveChunk(Message<byte[]> message) {

        try {

            byte[] body = message.body();

            // First four bytes indicate the json string length
            ByteBuffer byteBuffer = ByteBuffer.wrap(body, 0, 4);
            int len = byteBuffer.getInt();

            // Decode json
            int from = 4;
            byte[] jsonBytes = Arrays.copyOfRange(body, from, from + len);
            String jsonString = decode(jsonBytes);
            JsonObject jsonObject = new JsonObject(jsonString);

            // Remaining bytes are the chunk to be written
            from += len;
            byte[] data = Arrays.copyOfRange(body, from, body.length);

            saveChunk(message, jsonObject, data);

        } catch (RuntimeException e) {
            sendError(message, "error parsing byte[] message", e);
        }

    }

    public void saveChunk(Message<byte[]> message, JsonObject jsonObject, byte[] data) {

        if (data == null || data.length == 0) {
            sendError(message, "chunk data is null or empty");
            return;
        }

        String files_id = jsonObject.getString("files_id");
        if (files_id == null) {
            sendError(message, "files_id is missing");
            return;
        }
        ObjectId id = getObjectId(message, files_id);
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

    public void getMetaData(Message<JsonObject> message) {

        try {
            GridFSDBFile file = getFile(message);
            JsonObject fileInfo = new JsonObject();

            fileInfo.putString("filename", file.getFilename())
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

        } catch (RuntimeException e) {
            sendError(message, "Runtime error", e);
        }

    }

    public void getChunk(Message<JsonObject> message) {

        try {
            GridFSDBFile file = getFile(message);

            if (file == null) {
                return;
            }

            Integer n = message.body().getInteger("n");
            if (n == null) {
                sendError(message, "n (chunk number) is required");
                return;
            }
            if (n < 0) {
                sendError(message, "n must be greater than or equal to zero");
                return;
            }

            int len = (int) file.getChunkSize();
            String bucket = message.body().getString("bucket", GridFS.DEFAULT_BUCKET);

            DBCollection collection = db.getCollection(bucket + ".chunks");
            DBObject dbObject = BasicDBObjectBuilder.start("files_id", file.getId())
                    .add("n", n).get();

            DBObject result = collection.findOne(dbObject);

            if (result == null) {
                message.reply(new byte[0]);
                return;
            }

            byte[] data = (byte[]) result.get("data");
            message.reply(data);

        } catch (Throwable e) {
            sendError(message, "Error getting byte range", e);
        }

    }

    public GridFSDBFile getFile(Message<JsonObject> message) {

        String id = message.body().getString("id", null);
        if (id == null) {
            sendError(message, "id is required");
            return null;
        }

        // Optional bucket, default is "fs"
        String bucket = message.body().getString("bucket", GridFS.DEFAULT_BUCKET);

        GridFS files = new GridFS(db, bucket);

        ObjectId objectId = getObjectId(message, id);
        if (objectId == null) {
            return null;
        }

        GridFSDBFile file = files.findOne(objectId);

        if (file == null) {
            sendError(message, "File does not exist: " + id);
        }

        return file;
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

    private ObjectId getObjectId(Message message, String id) {
        try {
            return new ObjectId(id);
        } catch (Exception e) {
            sendError(message, "id " + id + " is not a valid ObjectId", e);
            return null;
        }
    }

}
