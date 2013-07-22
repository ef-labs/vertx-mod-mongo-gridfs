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
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.testtools.TestVerticle;

import java.util.Date;

import static org.vertx.testtools.VertxAssert.*;

/**
 * Integration tests for the saveFile operation
 */
public class SaveFileIntegrationTest extends TestVerticle {

    private EventBus eventBus;
    private JsonObject config;
    private final String address = GridFSModule.DEFAULT_ADDRESS;

    @Test
    public void testSaveFile_Empty_Json() {

        eventBus.send(address, new JsonObject(), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                IntegrationTestHelper.verifyErrorReply(message, "action must be specified");
            }
        });

    }

    @Test
    public void testSaveFile_Missing_ID() {

        JsonObject message = new JsonObject()
                .putString("action", "saveFile");

        eventBus.send(address, message, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                IntegrationTestHelper.verifyErrorReply(message, "id must be specified");
            }
        });

    }

    @Test
    public void testSaveFile_Missing_Length() {

        JsonObject message = new JsonObject()
                .putString("action", "saveFile")
                .putString("id", new ObjectId().toString());

        eventBus.send(address, message, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                IntegrationTestHelper.verifyErrorReply(message, "length must be specified");
            }
        });

    }

    @Test
    public void testSaveFile_Missing_ChunkSize() {

        JsonObject message = new JsonObject()
                .putString("action", "saveFile")
                .putString("id", new ObjectId().toString())
                .putNumber("length", 1024000);

        eventBus.send(address, message, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                IntegrationTestHelper.verifyErrorReply(message, "chunkSize must be specified");
            }
        });

    }

    @Test
    public void testSaveFile_All() {

        final ObjectId id = new ObjectId();
        final int length = 1024000;
        final int chunkSize = 102400;
        final long uploadDate = System.currentTimeMillis();
        final String filename = "integration_test.jpg";
        final String contentType = "image/jpeg";
        final String bucket = "it";

        JsonObject message = new JsonObject()
                .putString("action", "saveFile")
                .putString("id", id.toString())
                .putNumber("length", length)
                .putNumber("chunkSize", chunkSize)
                .putNumber("uploadDate", uploadDate)
                .putString("filename", filename)
                .putString("contentType", contentType)
                .putString("bucket", bucket)
                .putObject("metadata", new JsonObject().putString("additional", "info"));

        eventBus.send(address, message, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                assertEquals("ok", message.body().getString("status"));

                GridFS gridFS = IntegrationTestHelper.getGridFS(config, bucket);
                GridFSDBFile file = gridFS.find(id);
                assertNotNull(file);
                assertEquals(length, file.getLength());
                assertEquals(chunkSize, file.getChunkSize());
                assertEquals(new Date(uploadDate), file.getUploadDate());
                assertEquals(filename, file.getFilename());
                assertEquals(contentType, file.getContentType());

                DBObject metadata = file.getMetaData();
                assertNotNull(metadata);
                assertEquals("info", metadata.get("additional"));

                testComplete();
            }
        });

    }

    @Override
    public void start(Future<Void> startedResult) {
        eventBus = vertx.eventBus();
        config = IntegrationTestHelper.onVerticleStart(this, startedResult);
    }

}
