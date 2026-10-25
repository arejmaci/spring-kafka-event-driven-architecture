package com.benja.course.springcloud.kafka.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Punto de entrada de la aplicación API de productos.
 */
@SpringBootApplication
public class ProductsApiApplication {

    /**
     * Inicia la aplicación Spring Boot.
     *
     * @param args argumentos recibidos al ejecutar la aplicación
     */
    public static void main(String[] args) {
        SpringApplication.run(ProductsApiApplication.class, args);
    }

}
