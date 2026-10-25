package com.benja.course.springcloud.kafka.api.model.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Contiene los datos de un producto recibidos desde una solicitud de la API.
 *
 * @param name nombre obligatorio del producto
 * @param price precio obligatorio, cuyo valor mínimo permitido es 10
 */
public record ProductDto(@NotBlank String name, @NotNull @Min(10) Double price) {
}
