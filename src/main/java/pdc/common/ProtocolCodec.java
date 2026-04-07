package pdc.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ProtocolCodec {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ProtocolCodec() {
    }

    public static void writeEnvelope(DataOutputStream output, MessageEnvelope envelope) throws IOException {
        byte[] raw = MAPPER.writeValueAsString(envelope).getBytes(StandardCharsets.UTF_8);
        output.writeInt(raw.length);
        output.write(raw);
        output.flush();
    }

    public static MessageEnvelope readEnvelope(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > 16 * 1024 * 1024) {
            throw new IOException("Invalid message length: " + length);
        }
        byte[] raw = input.readNBytes(length);
        if (raw.length != length) {
            throw new IOException("Incomplete message payload");
        }
        return MAPPER.readValue(raw, MessageEnvelope.class);
    }

    public static <T> T payloadAs(Object payload, Class<T> clazz) {
        return MAPPER.convertValue(payload, clazz);
    }

    public static String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize object", e);
        }
    }
}
