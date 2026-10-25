package com.benja.course.springcloud.kafka.api.model;

/**
 * Representa una respuesta genérica recibida desde otro servicio.
 *
 * @param <T> tipo de dato incluido en el cuerpo de la respuesta
 * @param status estado de la operación
 * @param message mensaje descriptivo de la respuesta
 * @param body contenido de la respuesta
 */
public record Reply<T>(String status, String message, T body) {
}
