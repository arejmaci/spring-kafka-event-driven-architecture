package com.benja.course.springcloud.kafka.api.controllers;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Expone un endpoint sencillo para comprobar que la API está disponible.
 */
@RestController
public class PingController {

    /**
     * Responde a la solicitud de comprobación de disponibilidad.
     *
     * @return mensaje que indica que la API está activa
     */
    @GetMapping("/ping")
    public String ping() {
        return "Ping Ok Product Api";
    }

}
