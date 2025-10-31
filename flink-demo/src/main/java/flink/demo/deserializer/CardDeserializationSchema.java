package flink.demo.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import flink.demo.model.Card;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

@Slf4j
public class CardDeserializationSchema implements DeserializationSchema<Card> {

    @Override
    public Card deserialize(byte[] message) throws IOException {
        JsonNode after = DeserializationUtil.getAfterJsonNode(message);
        String cardId;
        boolean autoPayment = false;
        if (after != null) {
            log.info("Card received: {}", after);
            cardId = after.get("card_id").asText();
            autoPayment = after.get("auto_payment").asBoolean();
        } else {
            JsonNode before = DeserializationUtil.getBeforeJsonNode(message);
            log.info("Card received: {}", before);
            if (before == null) return null;
            cardId = before.get("card_id").asText();
        }

        return Card.builder()
                .cardId(cardId)
                .autoPayment(autoPayment)
                .build();
    }

    @Override
    public boolean isEndOfStream(Card nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Card> getProducedType() {
        return TypeInformation.of(Card.class);
    }
}

