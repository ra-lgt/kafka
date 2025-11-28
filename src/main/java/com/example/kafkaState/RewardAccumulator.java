package com.example.kafkaState;

public class RewardAccumulator {
    private String customerId;
    private double totalRewardPoints;

    public RewardAccumulator() {
    }

    public RewardAccumulator(String customerId, double totalRewardPoints) {
        this.customerId = customerId;
        this.totalRewardPoints = totalRewardPoints;
    }

    public String getCustomerId() {
        return customerId;
    }

    public double getTotalRewardPoints() {
        return totalRewardPoints;
    }

    @Override
    public String toString() {
        return "RewardAccumulator{customerId='" + customerId + '\'' +
                ", totalRewardPoints=" + totalRewardPoints + '}';
    }
}
