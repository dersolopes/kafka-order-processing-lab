package com.kafkaorder.producer.service;

import com.kafkaorder.producer.dto.CreateOrderRequest;
import com.kafkaorder.producer.dto.CreateOrderResponse;
import com.kafkaorder.producer.dto.OrderStatus;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class OrderService {

    public CreateOrderResponse createOrder(CreateOrderRequest request) {

        String orderId = UUID.randomUUID().toString();

        return new CreateOrderResponse(orderId, OrderStatus.RECEIVED);
    }
}