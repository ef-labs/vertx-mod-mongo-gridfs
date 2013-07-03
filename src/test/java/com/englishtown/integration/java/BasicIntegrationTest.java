package com.englishtown.integration.java;

import com.englishtown.GridFSModule;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.eventbus.impl.JsonObjectMessage;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import java.io.InputStream;

import static org.vertx.testtools.VertxAssert.*;

/**
 * Simple integration test which shows tests deploying other verticles, using the Vert.x API etc
 */
public class BasicIntegrationTest extends TestVerticle {

    @Test
    public void testWriteFile() throws Exception {

        ObjectId id = new ObjectId();

        writeFile(id, new Handler<Boolean>() {
            @Override
            public void handle(Boolean result) {
                if (result) {
                    testComplete();
                } else {
                    fail();
                }
            }
        });
    }

    @Test
    public void testWriteAndReadFile() throws Exception {

        final ObjectId id = new ObjectId();

        final Handler<Boolean> readDoneHandler = new Handler<Boolean>() {
            @Override
            public void handle(Boolean result) {
                if (result) {
                    testComplete();
                } else {
                    fail();
                }
            }
        };

        Handler<Boolean> writeDoneHandler = new Handler<Boolean>() {
            @Override
            public void handle(Boolean result) {
                if (result) {
                    readFile(id, readDoneHandler);
                } else {
                    fail();
                }
            }
        };

        writeFile(id, writeDoneHandler);

    }

    private void writeFile(ObjectId id, final Handler<Boolean> doneHandler) throws Exception {

        String files_id = id.toString();

        int chunkSize = 102400;
        byte[] bytes = new byte[chunkSize];

        final SaveResults results = new SaveResults();
        // At least one reply for saving file info
        results.expectedReplies++;

        Handler<Message<JsonObject>> replyHandler = new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                String status = reply.body().getString("status");
                assertEquals("ok", status);

                if ("ok".equals(status)) {
                    results.count++;
                    if (results.expectedReplies == results.count) {
                        doneHandler.handle(true);
                    }
                }
            }
        };

        InputStream inputStream = this.getClass().getResourceAsStream("/EF_Labs_ENG_logo.JPG");
        int len = inputStream.read(bytes);

        int n = 0;
        int totalLength = len;

        while (len > 0) {
            JsonObject jsonObject = new JsonObject()
                    .putString("files_id", files_id)
                    .putNumber("n", n++);

            byte[] jsonBytes = jsonObject.encode().getBytes("UTF-8");
            Buffer buffer = new Buffer(chunkSize + 4 + jsonBytes.length);

            buffer.appendInt(jsonBytes.length);
            buffer.appendBytes(jsonBytes);
            buffer.appendBytes(bytes);

            results.expectedReplies++;

            // Send chunk to event bus
            vertx.eventBus().send(GridFSModule.DEFAULT_ADDRESS + "/saveChunk", buffer.getBytes(), replyHandler);

            len = inputStream.read(bytes);
            if (len > 0) {
                totalLength += len;
            }
        }

        JsonObject fileInfo = new JsonObject()
                .putString("id", files_id)
                .putNumber("length", totalLength)
                .putNumber("chunkSize", chunkSize)
                .putNumber("uploadDate", System.currentTimeMillis())
                .putString("filename", "image.jpg")
                .putString("contentType", "image/jpeg");

        vertx.eventBus().send(GridFSModule.DEFAULT_ADDRESS + "/saveFileInfo", fileInfo, replyHandler);

    }

    private void readFile(final ObjectId id, final Handler<Boolean> doneHandler) {

        JsonObject message = new JsonObject().putString("id", id.toString());

        vertx.eventBus().send(GridFSModule.DEFAULT_ADDRESS + "/getFileInfo", message, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                String status = reply.body().getString("status");
                assertEquals("ok", status);
                if ("ok".equals(status)) {

                    final int chunkSize = reply.body().getInteger("chunkSize");
                    final int length = reply.body().getInteger("length");

                    JsonObject chunkMessage = new JsonObject()
                            .putString("id", id.toString())
                            .putNumber("n", 0);

                    vertx.eventBus().send(GridFSModule.DEFAULT_ADDRESS + "/getChunk", chunkMessage, new Handler<Message<byte[]>>() {
                        @Override
                        public void handle(Message<byte[]> reply) {
                            byte[] chunk = reply.body();
                            assertEquals(chunkSize, chunk.length);
                            testComplete();
                        }
                    });
                }
            }
        });

    }

    private static class SaveResults {
        public int expectedReplies;
        public int count;
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
