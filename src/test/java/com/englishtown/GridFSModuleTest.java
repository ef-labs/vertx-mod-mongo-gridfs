package com.englishtown;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link GridFSModule}
 */
@RunWith(MockitoJUnitRunner.class)
public class GridFSModuleTest {

    @Mock
    Message<byte[]> bytesMessage;

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testSaveChunk() throws Exception {

        Buffer buffer = new Buffer();
        String files_id = UUID.randomUUID().toString();
        int chunkNumber = 300;

        JsonObject jsonObject = new JsonObject()
                .putString("files_id", files_id)
                .putNumber("n", chunkNumber);

        byte[] jsonBytes = jsonObject.encode().getBytes("UTF-8");

        // The first four bytes are the json length
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(jsonBytes.length);
        buffer.appendBytes(byteBuffer.array());

        // Then the json bytes
        buffer.appendBytes(jsonBytes);

        // Then the binary data
        byte[] data = new byte[1024000];
        data[1023999] = 127;
        buffer.appendBytes(data);

        byte[] body = buffer.getBytes();
        when(bytesMessage.body()).thenReturn(body);

        GridFSModule module = new GridFSModule();
        module.saveChunk(bytesMessage);

    }
}
