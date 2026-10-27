package com.benja.course.springcloud.kafka.api.services;

import com.benja.course.springcloud.kafka.api.model.Reply;
import com.benja.course.springcloud.kafka.api.model.dto.ProductDto;

import java.time.Duration;

/**
 * Define las operaciones para enviar comandos de productos y obtener sus respuestas.
 */
public interface ProductCommandService {

    /**
     * Envía un comando para crear un producto y espera la respuesta hasta que
     * transcurra el tiempo límite indicado.
     *
     * @param dto datos del producto que se desea crear
     * @param timeout tiempo máximo de espera por la respuesta
     * @return respuesta recibida del servicio que procesa el comando
     */
    Reply<?> sendCreateAndWait(ProductDto dto, Duration timeout);

    /**
     * Envía un comando para consultar un producto y espera la respuesta hasta
     * que transcurra el tiempo límite indicado.
     *
     * @param id identificador del producto que se desea consultar
     * @param timeout tiempo máximo de espera por la respuesta
     * @return respuesta recibida del servicio que procesa el comando
     */
    Reply<?> sendReadAndWait(Long id, Duration timeout);

    /**
     * Envía un comando para consultar todos los productos y espera la respuesta
     * hasta que transcurra el tiempo límite indicado.
     *
     * @param timeout tiempo máximo de espera por la respuesta
     * @return respuesta recibida del servicio que procesa el comando
     */
    Reply<?> sendReadAllAndWait( Duration timeout);
}
