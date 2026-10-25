package com.benja.course.springcloud.kafka.api.handler;

import com.benja.course.springcloud.kafka.api.messaging.ReplyInbox;
import com.benja.course.springcloud.kafka.api.model.Reply;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import java.util.function.Consumer;

/**
 * Configura el consumidor de mensajes de respuesta recibidos desde Kafka.
 */
@Configuration
public class RepliesConsumer {

    private final ReplyInbox replyInbox;

    /**
     * Crea el consumidor con la bandeja donde se registran las respuestas pendientes.
     *
     * @param replyInbox componente que asocia las respuestas con sus solicitudes
     */
    public RepliesConsumer(ReplyInbox replyInbox) {
        this.replyInbox = replyInbox;
    }

    /**
     * Crea la función que procesa las respuestas y completa la solicitud correspondiente.
     *
     * @return consumidor de mensajes con respuestas genéricas
     */
    @Bean
    public Consumer<Message<Reply<?>>> handleReplyConsumer() {
        return message -> {
            String correlationId = message.getHeaders().get("correlationId").toString();
            replyInbox.complete(correlationId, message.getPayload());
        };
    }
}
