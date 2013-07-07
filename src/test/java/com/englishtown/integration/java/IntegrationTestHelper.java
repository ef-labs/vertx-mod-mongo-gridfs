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

package com.englishtown.integration.java;

import com.englishtown.vertx.GridFSModule;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.fail;
import static org.vertx.testtools.VertxAssert.testComplete;

/**
 * Base verticle for GridFS integration tests
 */
public class IntegrationTestHelper {

    public static final String DEFAULT_CONTENT_TYPE = "image/jpeg";
    public static final String DEFAULT_FILENAME = "test_file.jpg";
    public static final Integer DEFAULT_CHUNK_SIZE = 102400;
    public static final BasicDBObject DEFAULT_METADATA = new BasicDBObject("additional", "info");
    public static final Integer DEFAULT_LENGTH = 161966;

    public static JsonObject onVerticleStart(final Verticle verticle, final Future<Void> startedResult) {

        JsonObject config = loadConfig();
        verticle.getContainer().deployVerticle(GridFSModule.class.getName(), config, new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> result) {
                if (result.succeeded()) {
                    startedResult.setResult(null);
                    verticle.start();
                } else {
                    startedResult.setFailure(result.cause());
                }
            }
        });

        return config;

    }

    public static GridFS getGridFS(JsonObject config, String bucket) {

        Mongo mongo;
        try {
            mongo = new MongoClient(config.getString("host", "localhost"), config.getInteger("port", 27017));
        } catch (UnknownHostException e) {
            fail();
            return null;
        }

        String dbName = config.getString("db_name", "default_db");
        DB db = mongo.getDB(dbName);

        if (bucket == null) {
            return new GridFS(db);
        } else {
            return new GridFS(db, bucket);
        }

    }

    public static String createFile(JsonObject config, String bucket) {

        GridFS gridFS = IntegrationTestHelper.getGridFS(config, bucket);
        GridFSInputFile inputFile = gridFS.createFile(IntegrationTestHelper.class.getResourceAsStream("/EF_Labs_ENG_logo.JPG"));

        inputFile.setContentType(DEFAULT_CONTENT_TYPE);
        inputFile.setFilename(DEFAULT_FILENAME);
        inputFile.setChunkSize(DEFAULT_CHUNK_SIZE);
        inputFile.setMetaData(DEFAULT_METADATA);
        inputFile.save();

        return inputFile.getId().toString();
    }

    private static JsonObject loadConfig() {

        try (InputStream stream = IntegrationTestHelper.class.getResourceAsStream("/config.json")) {
            StringBuilder sb = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));

            String line = reader.readLine();
            while (line != null) {
                sb.append(line).append('\n');
                line = reader.readLine();
            }

            return new JsonObject(sb.toString());

        } catch (IOException e) {
            e.printStackTrace();
            fail();
            return new JsonObject();
        }

    }

    public static void verifyErrorReply(Message<JsonObject> message, String error) {
        assertEquals("error", message.body().getString("status"));
        assertEquals(error, message.body().getString("message"));
        testComplete();
    }

}
