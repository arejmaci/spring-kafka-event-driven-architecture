package com.benja.course.springcloud.kafka.command.handlers;

import com.benja.course.springcloud.kafka.command.model.Command;
import com.benja.course.springcloud.kafka.command.model.Reply;
import com.benja.course.springcloud.kafka.command.model.dto.ProductDto;
import com.benja.course.springcloud.kafka.command.services.ProductService;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.function.Function;

import org.slf4j.Logger;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

@Configuration
public class ProductCommandConsumer {

    private final ProductService productService;
    private static final Logger logger = LoggerFactory.getLogger(ProductCommandConsumer.class.getName());

    public ProductCommandConsumer(ProductService productService) {
        this.productService = productService;
    }

    @Bean
    public Function<Message<Command<ProductDto>>, Message<Reply<?>>> handlerCommands() {

        return msg -> {
            Command<ProductDto> command = msg.getPayload();
            String commandType = command.type() == null ? "null" : command.type().toUpperCase();
            Reply<?> reply = null;
            switch (commandType) {
                case "CREATE" -> {
                    if (command.body() == null) {
                        logger.warn("Received CREATE command with null body");
                        reply = new Reply<>("ERROR", "Empty Body",null);
                    } else {
                        ProductDto productDto = command.body();
                        ProductDto productSave = productService.createProduct(command.body());
                        logger.info("Creating product name={}, price={}", productSave.name(),productSave.price());
                        reply = new Reply<>("SUCCESS", "Created Product",productSave);
                    }
                }
                case "READ" -> {
                    if (command.id() == null) {
                        logger.warn("id is required");
                    } else {
                        ProductDto productRead = productService.findById(command.id());
                        logger.info("Reading product by ID: {}", command.id());
                        reply = (productRead == null)?
                                new Reply<>("ERROR", "There is not product with id = " + command.id(),null):
                                new Reply<>("SUCCESS", "Read Product:",productRead);
                    }
                }
                case "READ_ALL" -> {
                    logger.info("Reading all products");

                    reply = new Reply<>("SUCCESS", "Read Product:", productService.findAll());
                }
//                case "UPDATE" -> {
//                    if (command.body() == null) {
//                        logger.warn("Received UPDATE command with null body");
//                    } else {
//                        logger.info("Updating product: {}", command.body());
//                    }
//                }
//                case "DELETE" -> {
//                    if (command.body() == null) {
//                        logger.warn("Received DELETE command with null body");
//                    } else {
//                        logger.info("Deleting product: {}", command.body());
//                    }
//                }
                default -> {
                    logger.warn("Unknown command type: {}", command.type());
                    reply = new Reply<>("ERROR", "Unknown command type",null);
                }
            }
            String correlationId = msg.getHeaders().get("correlationId").toString();
            logger.info("CorrelationId={} recibido", correlationId);
            MessageBuilder<Reply<?>> builderOut = MessageBuilder.withPayload(reply);
            if (correlationId != null) {
                builderOut.setHeader("correlationId", correlationId);
            }
            return builderOut.build();
        };
    }
}
