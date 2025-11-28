package com.example.kafkaState;

import com.example.kafkaState.RewardAccumulator;
import com.example.kafkaState.Purchase;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class RewardTransformer implements ValueTransformer<Purchase, RewardAccumulator> {

    private KeyValueStore<String, Double> rewardStore;    // user-123 : 1

    @Override
    public void init(ProcessorContext context) {
        rewardStore = (KeyValueStore<String, Double>) context.getStateStore("rewardStore");
    }

    @Override
    public RewardAccumulator transform(Purchase purchase) {

        double oldPoints = rewardStore.get(purchase.getCustomerId()) == null
                ? 0.0
                : rewardStore.get(purchase.getCustomerId());

        double newPoints = oldPoints + purchase.getAmount();

        rewardStore.put(purchase.getCustomerId(), newPoints);

        return new RewardAccumulator(purchase.getCustomerId(), newPoints);
    }

    @Override
    public void close() {}
}