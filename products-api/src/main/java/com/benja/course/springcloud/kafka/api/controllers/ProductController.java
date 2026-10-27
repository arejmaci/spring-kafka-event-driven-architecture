package com.benja.course.springcloud.kafka.api.controllers;

import com.benja.course.springcloud.kafka.api.model.Reply;
import com.benja.course.springcloud.kafka.api.model.dto.ProductDto;
import jakarta.validation.Valid;
import org.jspecify.annotations.NonNull;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.benja.course.springcloud.kafka.api.services.ProductCommandService;

import java.time.Duration;
import java.util.Map;

/**
 * Expone endpoints HTTP para crear y consultar productos, devolviendo
 * los resultados recibidos del servicio que procesa los comandos.
 */
@RestController
@RequestMapping("/products")
public class ProductController {
    private final ProductCommandService productCommandService;

    /**
     * Crea el controlador con el servicio que envía comandos y espera sus respuestas.
     *
     * @param productCommandService servicio de envío de comandos de productos
     */
    public ProductController(ProductCommandService productCommandService) {
        this.productCommandService = productCommandService;
    }

    /**
     * Solicita la creación de un producto y devuelve el resultado recibido
     * del servicio que procesa el comando, con un máximo de cinco segundos de espera.
     *
     * @param productDto datos validados del producto
     * @return respuesta HTTP con el producto creado o el error informado
     */
    @PostMapping
    public ResponseEntity<?> create(@Valid @RequestBody ProductDto productDto) {
        Reply<?> reply = productCommandService.sendCreateAndWait(productDto, Duration.ofSeconds(5) );
        return getResponseEntity(reply);
    }

    /**
     * Solicita la consulta de un producto y devuelve el resultado recibido
     * del servicio que procesa el comando, con un máximo de cinco segundos de espera.
     *
     * @param id identificador del producto que se desea consultar
     * @return respuesta HTTP con los datos del producto o el error informado
     */
    @GetMapping("/{id}")
    public ResponseEntity<?> getById(@PathVariable Long id) {
        Reply<?> reply = productCommandService.sendReadAndWait(id, Duration.ofSeconds(5) );
        return getResponseEntity(reply);
    }

    /**
     * Solicita la consulta de todos los productos y espera hasta cinco segundos
     * por la respuesta del servicio que procesa el comando.
     *
     * @return respuesta HTTP con la lista de productos o el error informado
     */
    @GetMapping()
    public ResponseEntity<?> getAll() {
        Reply<?> reply = productCommandService.sendReadAllAndWait(Duration.ofSeconds(5) );
        return getResponseEntity(reply);
    }

    /**
     * Convierte la respuesta del servicio en una respuesta HTTP según su estado:
     * devuelve el cuerpo si la operación tuvo éxito o el mensaje de error si falló.
     *
     * @param reply respuesta recibida del servicio que procesa el comando
     * @return respuesta HTTP con estado 200 para éxito o 400 para error
     */
    private static @NonNull ResponseEntity<?> getResponseEntity(Reply<?> reply) {
        if("SUCCESS".equalsIgnoreCase(reply.status())){
            return ResponseEntity.ok().body(reply.body());
        }
        return ResponseEntity.badRequest().body((Map.of("Error", reply.message())));
    }
}
