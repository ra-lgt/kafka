package com.example.kafkaState;

public class Purchase {
    private String customerId;
    private double amount;

    public Purchase() {
    }

    public Purchase(String customerId, double amount) {
        this.customerId = customerId;
        this.amount = amount;
    }

    public String getCustomerId() {
        return customerId;
    }

    public double getAmount() {
        return amount;
    }
}
