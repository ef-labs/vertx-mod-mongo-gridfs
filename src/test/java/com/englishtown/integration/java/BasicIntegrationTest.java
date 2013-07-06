package com.englishtown.integration.java;

import com.englishtown.GridFSModule;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.vertx.testtools.VertxAssert.*;

/**
 * Integration test showing a GridFS write and then reading it back
 */
public class BasicIntegrationTest extends TestVerticle {

    @Test
    public void testWriteAndReadFile() throws Exception {

        // Create a MongoDB ObjectId
        final ObjectId id = new ObjectId();

        // Handler for when read is complete
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

        // Handler for when write is complete and then triggers the read to start
        Handler<Boolean> writeDoneHandler = new Handler<Boolean>() {
            @Override
            public void handle(Boolean result) {
                if (result) {
                    startReadFile(id, readDoneHandler);
                } else {
                    fail();
                }
            }
        };

        // Start the write operation
        startWriteFile(id, writeDoneHandler);

    }

    private void startWriteFile(ObjectId id, final Handler<Boolean> doneHandler) throws Exception {

        String files_id = id.toString();

        // Save image in chunks of 100k
        int chunkSize = 102400;
        byte[] bytes = new byte[chunkSize];

        final SaveResults results = new SaveResults();
        // At least one reply for saving file info
        results.expectedReplies++;

        // Reply handler for writing chunks and the file info
        Handler<Message<JsonObject>> replyHandler = new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                String status = reply.body().getString("status");
                assertEquals("ok", status);

                if ("ok".equals(status)) {
                    results.count++;
                    // Check if we're done
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
                .putString("action", "saveFile")
                .putString("id", files_id)
                .putNumber("length", totalLength)
                .putNumber("chunkSize", chunkSize)
                .putNumber("uploadDate", System.currentTimeMillis())
                .putString("filename", "image.jpg")
                .putString("contentType", "image/jpeg");

        vertx.eventBus().send(GridFSModule.DEFAULT_ADDRESS, fileInfo, replyHandler);

    }

    private void startReadFile(final ObjectId id, final Handler<Boolean> doneHandler) {

        JsonObject message = new JsonObject().putString("id", id.toString()).putString("action", "getFile");

        vertx.eventBus().send(GridFSModule.DEFAULT_ADDRESS, message, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                String status = reply.body().getString("status");
                assertEquals("ok", status);
                if ("ok".equals(status)) {

                    final int chunkSize = reply.body().getInteger("chunkSize");
                    final int length = reply.body().getInteger("length");

                    JsonObject chunkMessage = new JsonObject()
                            .putString("action", "getChunk")
                            .putString("files_id", id.toString())
                            .putNumber("n", 0);

                    vertx.eventBus().send(GridFSModule.DEFAULT_ADDRESS, chunkMessage, new Handler<Message<byte[]>>() {
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
     * {@inheritDoc}
     */
    @Override
    public void start(final Future<Void> startedResult) {

        JsonObject config = loadConfig();
        container.deployVerticle(GridFSModule.class.getName(), config, new Handler<AsyncResult<String>>() {
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

    private JsonObject loadConfig() {

        try (InputStream stream = this.getClass().getResourceAsStream("/config.json")) {
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

}
