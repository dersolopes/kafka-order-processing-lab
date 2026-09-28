package com.kafkaorder.producer.controller;

import com.kafkaorder.producer.dto.CreateOrderRequest;
import com.kafkaorder.producer.dto.CreateOrderResponse;
import com.kafkaorder.producer.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;


    // @RequestMapping("/orders") e @PostMapping resultam em POST /orders
    @PostMapping
    public CreateOrderResponse createOrder(
            @Valid @RequestBody CreateOrderRequest order) {

    return orderService.createOrder(order);
    }
}