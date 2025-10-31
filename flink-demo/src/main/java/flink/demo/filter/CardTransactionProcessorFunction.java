package flink.demo.filter;

import flink.demo.model.Card;
import flink.demo.model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class CardTransactionProcessorFunction extends KeyedBroadcastProcessFunction<String, Transaction, Card, Transaction> {

    private final MapStateDescriptor<String, Card> cardStateDescriptor;

    public CardTransactionProcessorFunction(MapStateDescriptor<String, Card> cardStateDescriptor) {
        this.cardStateDescriptor = cardStateDescriptor;
    }

    // For processing cards stream and updating card state
    @Override
    public void processBroadcastElement(Card card, Context ctx, Collector<Transaction> out) throws Exception {
        log.info("Broadcast: Card received: {}", card);

        if (card == null || card.getCardId() == null) {
            return;
        }

        BroadcastState<String, Card> broadcastState = ctx.getBroadcastState(cardStateDescriptor);

        if (card.isAutoPayment()) {
            broadcastState.put(card.getCardId(), card);
            log.info("RocksDB UPDATE - Card added/updated: {}", card.getCardId());
        } else {
            if (broadcastState.contains(card.getCardId())) {
                broadcastState.remove(card.getCardId());
                log.info("RocksDB DELETE - Card removed: {}", card.getCardId());
            }
        }

        broadcastState.iterator()
                .forEachRemaining(entry -> log.info("Broadcast State: Key={}, Value={}", entry.getKey(), entry.getValue()));
    }

    // For filtering transactions
    @Override
    public void processElement(Transaction transaction, ReadOnlyContext ctx, Collector<Transaction> out) throws Exception {
        log.info("Process: Transaction received: {}", transaction);

        if (transaction == null || transaction.getCardId() == null || transaction.getCardId().isEmpty()) {
            return;
        }

        // only for filtering
        ReadOnlyBroadcastState<String, Card> broadcastState = ctx.getBroadcastState(cardStateDescriptor);

        if (broadcastState.contains(transaction.getCardId())) {
            out.collect(transaction);  // Filter passed, send to downstream
            log.info("Transaction APPROVED - Card: {}, AutoPayment: true", transaction.getCardId());
        }
    }
}