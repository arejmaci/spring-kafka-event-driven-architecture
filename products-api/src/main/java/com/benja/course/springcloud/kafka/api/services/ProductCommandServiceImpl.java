package com.benja.course.springcloud.kafka.api.services;

import com.benja.course.springcloud.kafka.api.model.Command;
import com.benja.course.springcloud.kafka.api.model.dto.ProductDto;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class ProductCommandServiceImpl implements ProductCommandService {

    private final StreamBridge streamBridge;

    public ProductCommandServiceImpl(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }

    @Override
    public void sendCreate(ProductDto dto) {
        Command<ProductDto> command = new Command<>("CREATE", null, dto);
        String correlationId = UUID.randomUUID().toString();
        Message<Command<ProductDto>> msg = MessageBuilder.withPayload(command)
                .setHeader("correlationId",correlationId).build();

            boolean sent = streamBridge.send("productCommand-out-0", msg);
        if (!sent) {
            throw new IllegalStateException("Failed to send command to kafka topic");
        }
    }
}
