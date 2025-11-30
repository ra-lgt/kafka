package com.example.kafkaTable;

public class ProductSale {
    private String productId;
    private int quantity;

    public ProductSale() {}

    public ProductSale(String productId, int quantity) {
        this.productId = productId;
        this.quantity = quantity;
    }

    public String getProductId() { return productId; }
    public int getQuantity() { return quantity; }
}