package com.benja.course.springcloud.kafka.api.messaging;

import com.benja.course.springcloud.kafka.api.model.Reply;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mantiene las solicitudes pendientes y permite completar cada una
 * cuando llega su respuesta con el identificador de correlación correspondiente.
 */
@Component
public class ReplyInbox {
    // Guarda consultas que estan esperando respuestas.
    private final ConcurrentHashMap<String, CompletableFuture<Reply<?>>> messagePending = new ConcurrentHashMap<>();

    /**
     * Registra una solicitud pendiente asociada a un identificador de correlación.
     *
     * @param correlationId identificador único de la solicitud
     * @return futuro que se completará cuando llegue la respuesta
     */
    public CompletableFuture<Reply<?>> Register(String correlationId) {
        CompletableFuture<Reply<?>> future = new CompletableFuture<>();
        messagePending.put(correlationId, future);
        return future;
    };

    /**
     * Busca la solicitud pendiente y completa su futuro con la respuesta recibida.
     *
     * @param correlationId identificador de correlación de la solicitud
     * @param reply respuesta que se entregará a la solicitud pendiente
     */
    public void complete(String correlationId, Reply<?> reply) {
        if (correlationId == null) {
            throw new NullPointerException("correlationId no puede ser nulo");
        }
        CompletableFuture<Reply<?>> future = messagePending.remove(correlationId);
        if (future != null){
            future.complete(reply);
        }
    }
}
