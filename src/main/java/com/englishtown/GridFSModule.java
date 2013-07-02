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

import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Arrays;

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

        eb.registerHandler(address + "/getByteRange", new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                getByteRange(message);
            }
        });

    }

    @Override
    public void stop() {
        mongo.close();
    }

    public void getMetaData(Message<JsonObject> message) {

        try {
            GridFSDBFile file = getFile(message);
            JsonObject fileInfo = new JsonObject();

            fileInfo.putString("status", "ok")
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
            message.reply(fileInfo);

        } catch (RuntimeException e) {
            sendError(message, "Runtime error", e);
        }

    }

    public void getByteRange(Message<JsonObject> message) {

        Integer from = message.body().getInteger("from");
        Integer to = message.body().getInteger("to");

        if (from == null || to == null) {
            sendError(message, "from and to fields are required");
            return;
        }
        if (from < 0 || from >= to) {
            sendError(message, "from must be greater than zero and less than to");
            return;
        }

        GridFSDBFile file = getFile(message);

        if (file == null) {
            return;
        }

        int length = to - from + 1;
        byte[] bytes = new byte[length];

        try (InputStream stream = file.getInputStream()) {

            if (from > 0) {
                stream.skip(from);
            }

            int l = stream.read(bytes, 0, length);

            if (l == length) {
                message.reply(bytes);
            } else if (l > 0) {
                message.reply(Arrays.copyOfRange(bytes, 0, l));
            } else {
                message.reply(new byte[0]);
            }

        } catch (Throwable e) {
            sendError(message, "Error reading input stream", e);
        }

    }

    public GridFSDBFile getFile(Message<JsonObject> message) {

        String id = message.body().getString("id", null);
        if (id == null) {
            sendError(message, "id must be specified for getMetaData");
            return null;
        }

        // Optional bucket, default is "fs"
        String bucket = message.body().getString("bucket", GridFS.DEFAULT_BUCKET);

        GridFS files = new GridFS(db, bucket);
//        GridFSDBFile file = files.getMetaData(new BasicDBObject("_id", id));
        GridFSDBFile file = files.findOne(new ObjectId(id));

        if (file == null) {
            sendError(message, "File does not exist: " + id);
        }

        return file;
    }

    public void sendError(Message<JsonObject> message, String error) {
        sendError(message, error, null);
    }

    public void sendError(Message<JsonObject> message, String error, Throwable e) {
        logger.error(error, e);
        JsonObject result = new JsonObject().putString("status", "error").putString("message", error);
        message.reply(result);
    }

}
