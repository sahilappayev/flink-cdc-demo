package flink.demo.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import flink.demo.model.Transaction;
import java.io.IOException;
import java.math.BigDecimal;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

@Slf4j
public class TransactionDeserializationSchema implements DeserializationSchema<Transaction> {

    @Override
    public Transaction deserialize(byte[] message) throws IOException {
        JsonNode after = DeserializationUtil.getAfterJsonNode(message);

        if (after == null) {
            return null;
        }

        log.info("Transaction received: {}", after);

        String transactionId = after.get("transaction_id").asText();
        String cardId = after.get("card_id").asText();

        JsonNode amountNode = after.get("amount");
        BigDecimal amount = null;

        if (amountNode != null && amountNode.has("value") && amountNode.has("scale")) {
            String base64Value = amountNode.get("value").asText();
            int scale = amountNode.get("scale").asInt();

            amount = DecimalDecoder.decode(base64Value, scale);
        }

        return Transaction.builder()
                .transactionId(transactionId)
                .cardId(cardId)
                .amount(amount)
                .build();
    }

    @Override
    public boolean isEndOfStream(Transaction nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}

