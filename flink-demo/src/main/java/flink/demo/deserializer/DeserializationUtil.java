package flink.demo.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public class DeserializationUtil {

    private static final ObjectMapper objectMapper = new ObjectMapper();


    private DeserializationUtil() {
    }

    public static JsonNode getAfterJsonNode(byte[] message) throws IOException {
        JsonNode root = getRoot(message);
        if (root == null) return null;
        JsonNode after = root.get("after");

        if (after == null || after.isNull()) {
            return null;
        }
        return after;
    }

    private static JsonNode getRoot(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        return objectMapper.readTree(message);
    }

    public static JsonNode getBeforeJsonNode(byte[] message) throws IOException {
        JsonNode root = getRoot(message);
        if (root == null) return null;
        JsonNode before = root.get("before");
        if (before == null || before.isNull()) {
            return null;
        }
        return before;
    }
}
