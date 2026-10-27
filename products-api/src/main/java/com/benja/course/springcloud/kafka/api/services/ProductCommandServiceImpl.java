package com.benja.course.springcloud.kafka.api.services;

import com.benja.course.springcloud.kafka.api.messaging.ReplyInbox;
import com.benja.course.springcloud.kafka.api.model.Command;
import com.benja.course.springcloud.kafka.api.model.Reply;
import com.benja.course.springcloud.kafka.api.model.dto.ProductDto;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implementa el envío de comandos de creación y consulta de productos a Kafka,
 * y la espera de sus respuestas.
 */
@Service
public class ProductCommandServiceImpl implements ProductCommandService {

    private final StreamBridge streamBridge;
    private final ReplyInbox replyInbox;

    /**
     * Crea el servicio con los componentes necesarios para publicar comandos
     * y asociar las respuestas recibidas.
     *
     * @param streamBridge componente de Spring Cloud Stream para enviar mensajes
     * @param replyInbox componente que registra y completa las solicitudes pendientes
     */
    public ProductCommandServiceImpl(StreamBridge streamBridge, ReplyInbox replyInbox) {
        this.streamBridge = streamBridge;
        this.replyInbox = replyInbox;
    }

    /**
     * Envía a Kafka un comando de creación con un identificador de correlación único
     * y espera la respuesta asociada hasta que se cumpla el tiempo límite.
     *
     * @param dto datos del producto que se desea crear
     * @param timeout tiempo máximo de espera por la respuesta
     * @return respuesta recibida del servicio que procesa el comando
     * @throws IllegalStateException si no se logra enviar el comando a Kafka
     * @throws RuntimeException si la espera termina por tiempo agotado,
     *                           interrupción o error al obtener la respuesta
     */
    @Override
    public Reply<?> sendCreateAndWait(ProductDto dto, Duration timeout) {
        Command<ProductDto> command = new Command<>("CREATE", null, dto);
        return sendAndAwait(command, timeout);
    }

    /**
     * Envía a Kafka un comando de consulta y espera la respuesta asociada.
     *
     * @param id identificador del producto que se desea consultar
     * @param timeout tiempo máximo de espera por la respuesta
     * @return respuesta recibida del servicio que procesa el comando
     */
    @Override
    public Reply<?> sendReadAndWait(Long id, Duration timeout) {
        Command<ProductDto> command = new Command<>("READ", id, null);
        return sendAndAwait(command, timeout);
    }

    /**
     * Envía a Kafka un comando para consultar todos los productos y espera
     * la respuesta asociada hasta que se cumpla el tiempo límite.
     *
     * @param timeout tiempo máximo de espera por la respuesta
     * @return respuesta recibida del servicio que procesa el comando
     */
    @Override
    public Reply<?> sendReadAllAndWait(Duration timeout) {
        Command<ProductDto> command = new Command<>("READ_ALL", null, null);
        return sendAndAwait(command, timeout);
    }

    /**
     * Publica el comando con un identificador de correlación y espera su respuesta.
     * Registra la solicitud antes de publicarla para no perder una respuesta rápida.
     *
     * @param command comando que se enviará a Kafka
     * @param timeout tiempo máximo de espera por la respuesta
     * @return respuesta asociada al comando
     * @throws IllegalStateException si no se logra enviar el comando a Kafka
     * @throws RuntimeException si la espera termina por tiempo agotado,
     *                           interrupción o error al obtener la respuesta
     */
    private Reply<?> sendAndAwait(Command<ProductDto> command, Duration timeout) {
        String correlationId = UUID.randomUUID().toString();

        CompletableFuture<Reply<?>> future = replyInbox.Register(correlationId);

        Message<Command<ProductDto>> msg = MessageBuilder.withPayload(command)
                .setHeader("correlationId",correlationId).build();

        boolean sent = streamBridge.send("productCommand-out-0", msg);
        if (!sent) {
            throw new IllegalStateException("Failed to send command to kafka topic");
        }

        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Time out esperando la respuesta de product-command desde kafka", e);
        }
    }

}
