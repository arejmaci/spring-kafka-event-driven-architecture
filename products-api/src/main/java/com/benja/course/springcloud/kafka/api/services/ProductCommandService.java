package com.benja.course.springcloud.kafka.api.services;

import com.benja.course.springcloud.kafka.api.model.dto.ProductDto;

/**
 * Define las operaciones para enviar comandos relacionados con productos.
 */
public interface ProductCommandService {

    /**
     * Solicita la creación de un producto mediante el envío de un comando.
     *
     * @param dto datos del producto que se desea crear
     */
    void sendCreate(ProductDto dto);
}
