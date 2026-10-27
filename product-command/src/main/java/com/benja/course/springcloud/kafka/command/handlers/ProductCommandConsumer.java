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
                        ProductDto productSave = productService.createProduct(command.body());
                        logger.info("Creating product name={}, price={}", productSave.name(),productSave.price());
                        reply = new Reply<>("SUCCESS", "Created Product",productSave);
                    }
                }
                case "READ" -> {
                    if (command.id() == null) {
                        logger.warn("id is required");
                        reply = new Reply<>("ERROR", "Id is required.",null);
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
                case "UPDATE" -> {
                    if (command.body() == null || command.id() == null) {
                        logger.warn("Body and Id are required.");
                        reply = new Reply<>("ERROR", "Id and Body are required.",null);
                    } else {
                        logger.info("Updating product: {}", command.body());
                        ProductDto productUpdate = productService.updateProduct(command.body(),command.id());
                        if (productUpdate != null) {
                            reply = new Reply<>("SUCCESS", "Update Product:",productUpdate);
                            logger.info("Updating product name: {} price: {} ", command.body().name(), command.body().price());
                        } else {
                            reply = new Reply<>("ERROR", "There is not product with id = " + command.id() + " to update.",null);
                            logger.info("There is not product with id: {} ", command.id());
                        }
                    }
                }
                case "DELETE" -> {
                    if (command.id() == null) {
                        logger.warn("Received DELETE command with null id");
                        reply = new Reply<>("ERROR", "Received DELETE command with null id.",null);
                    } else {
                        logger.info("Deleting product: {}", command.id());
                        Boolean deleted = productService.deleteProduct(command.id());
                        if  (deleted) {
                            reply = new Reply<>("SUCCESS", "Deleted Product with id: " + command.id(), null);
                        } else{
                            reply = new Reply<>("ERROR", "There is not product with id = " + command.id() + " to delete.",null);
                            logger.info("There is not product with id: {} ", command.id());
                        }
                    }
                }
                default -> {
                    logger.warn("Unknown command type: {}", command.type());
                    reply = new Reply<>("ERROR", "Unknown command type",null);
                }
            }
            String correlationId = msg.getHeaders().get("correlationId",String.class);
            logger.info("CorrelationId={} recibido", correlationId);
            MessageBuilder<Reply<?>> builderOut = MessageBuilder.withPayload(reply);
            if (correlationId != null) {
                builderOut.setHeader("correlationId", correlationId);
            }
            return builderOut.build();
        };
    }
}
