package com.englishtown.integration.java;

import com.englishtown.GridFSModule;
import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.eventbus.impl.JsonObjectMessage;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.fail;
import static org.vertx.testtools.VertxAssert.testComplete;

/**
 * Simple integration test which shows tests deploying other verticles, using the Vert.x API etc
 */
public class BasicIntegrationTest extends TestVerticle {

    @Test
    public void testGetMetaData() throws Exception {

        JsonObject message = new JsonObject().putString("id", "51d347d4f671172940cc04b2");

        vertx.eventBus().send(GridFSModule.DEFAULT_ADDRESS + "/getMetaData", message, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> response) {
                assertEquals("ok", response.body().getString("status"));
                testComplete();
            }
        });

    }

    @Test
    public void testGetByteRange() throws Exception {

        JsonObject message = new JsonObject()
                .putString("id", "51d347d4f671172940cc04b2")
                .putNumber("from", 1024)
                .putNumber("to", 2047);

        vertx.eventBus().send(GridFSModule.DEFAULT_ADDRESS + "/getByteRange", message, new Handler<Message<byte[]>>() {
            @Override
            public void handle(Message<byte[]> response) {
                byte[] bytes = response.body();
                assertEquals(1024, bytes.length);
                testComplete();
            }
        });

    }

    @Test
    public void testGetByteRange_All() throws Exception {

        JsonObject message = new JsonObject()
                .putString("id", "51d347d4f671172940cc04b2")
                .putNumber("from", 0)
                .putNumber("to", 161965);

        vertx.eventBus().send(GridFSModule.DEFAULT_ADDRESS + "/getByteRange", message, new Handler<Message<byte[]>>() {
            @Override
            public void handle(Message<byte[]> response) {
                byte[] bytes = response.body();
                assertEquals(161966, bytes.length);
                testComplete();
            }
        });

    }

    @Test
    public void testGetByteRange_More() throws Exception {

        JsonObject message = new JsonObject()
                .putString("id", "51d347d4f671172940cc04b2")
                .putNumber("from", 0)
                .putNumber("to", 1619650);

        vertx.eventBus().send(GridFSModule.DEFAULT_ADDRESS + "/getByteRange", message, new Handler<Message<byte[]>>() {
            @Override
            public void handle(Message<byte[]> response) {
                byte[] bytes = response.body();
                assertEquals(161966, bytes.length);
                testComplete();
            }
        });

    }

    @Test
    public void testGetByteRange_Invalid_Range() throws Exception {

        JsonObject message = new JsonObject()
                .putString("id", "51d347d4f671172940cc04b2")
                .putNumber("from", 500)
                .putNumber("to", 100);

        vertx.eventBus().send(GridFSModule.DEFAULT_ADDRESS + "/getByteRange", message, new Handler<Message<byte[]>>() {
            @Override
            public void handle(Message<byte[]> response) {
                if ((Message)response instanceof JsonObjectMessage) {
                    JsonObjectMessage jsonObjectMessage = (JsonObjectMessage)(Message)response;
                    assertEquals("error", jsonObjectMessage.body().getString("status"));
                    testComplete();
                } else {
                    fail();
                }
            }
        });

    }

    /**
     * Override this method to signify that start is complete sometime _after_ the start() method has returned
     * This is useful if your verticle deploys other verticles or modules and you don't want this verticle to
     * be considered started until the other modules and verticles have been started.
     *
     * @param startedResult When you are happy your verticle is started set the result
     */
    @Override
    public void start(final Future<Void> startedResult) {

        container.deployVerticle(GridFSModule.class.getName(), new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> result) {
                if (result.succeeded()) {
                    startedResult.setResult(null);
                    BasicIntegrationTest.this.start();
                } else {
                    startedResult.setFailure(result.cause());
                }
            }
        });

    }
}
