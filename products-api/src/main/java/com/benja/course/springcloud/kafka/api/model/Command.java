package com.benja.course.springcloud.kafka.api.model;

/**
 * Representa un comando genérico que se envía para procesar una operación.
 *
 * @param <T> tipo de dato incluido en el cuerpo del comando
 * @param type tipo de operación solicitada
 * @param id identificador del recurso asociado, si corresponde
 * @param body contenido necesario para ejecutar el comando
 */
public record Command<T>(String type, Long id, T body) {
}
