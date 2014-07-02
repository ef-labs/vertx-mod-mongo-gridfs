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
import org.junit.Test;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.assertNotNull;
import static org.vertx.testtools.VertxAssert.testComplete;

/**
 * Integration tests for the getFile operation
 */
public class GetFileIntegrationTest extends TestVerticle {

    private EventBus eventBus;
    private JsonObject config;
    private final String address = GridFSModule.DEFAULT_ADDRESS;

    @Test
    public void testGetFile_Missing_Id() {

        JsonObject message = new JsonObject()
                .putString("action", "getFile");

        eventBus.send(address, message, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                IntegrationTestHelper.verifyErrorReply(message, "id must be specified");
            }
        });

    }

    @Test
    public void testGetFile() {

        String bucket = "it";
        String id = IntegrationTestHelper.createFile(config, bucket);

        JsonObject message = new JsonObject()
                .putString("action", "getFile")
                .putString("id", id)
                .putString("bucket", bucket);

        eventBus.send(address, message, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                JsonObject body = message.body();
                assertEquals("ok", body.getString("status"));
                assertEquals(IntegrationTestHelper.DEFAULT_CHUNK_SIZE, body.getInteger("chunkSize"));
                assertEquals(IntegrationTestHelper.DEFAULT_LENGTH, body.getInteger("length"));
                assertEquals(IntegrationTestHelper.DEFAULT_CONTENT_TYPE, body.getString("contentType"));
                assertEquals(IntegrationTestHelper.DEFAULT_FILENAME, body.getString("filename"));
                JsonObject metadata = body.getObject("metadata");
                assertNotNull(metadata);
                assertEquals("info", metadata.getString("additional"));
                testComplete();
            }
        });

    }

    @Override
    public void start(Future<Void> startedResult) {
        eventBus = vertx.eventBus();
        config = IntegrationTestHelper.onVerticleStart(this, startedResult, "/config.json");
    }

}
