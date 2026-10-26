package com.benja.course.springcloud.kafka.api.controllers;

import com.benja.course.springcloud.kafka.api.model.Reply;
import com.benja.course.springcloud.kafka.api.model.dto.ProductDto;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.benja.course.springcloud.kafka.api.services.ProductCommandService;

import java.time.Duration;
import java.util.Map;

/**
 * Expone los endpoints HTTP para gestionar solicitudes de productos.
 */
@RestController
@RequestMapping("/products")
public class ProductController {
    private final ProductCommandService productCommandService;

    /**
     * Crea el controlador con el servicio que envía comandos de producto.
     *
     * @param productCommandService servicio de envío de comandos
     */
    public ProductController(ProductCommandService productCommandService) {
        this.productCommandService = productCommandService;
    }

    /**
     * Solicita la creación de un producto y devuelve el resultado recibido
     * del servicio que procesa el comando.
     *
     * @param productDto datos validados del producto
     * @return respuesta HTTP con el producto creado o el error informado
     */
    @PostMapping
    public ResponseEntity<?> createProduct(@Valid @RequestBody ProductDto productDto) {
        Reply<?> reply = productCommandService.sendCreateAndWait(productDto, Duration.ofSeconds(5) );
        if("SUCCESS".equalsIgnoreCase(reply.status())){
            return ResponseEntity.ok().body(reply.body());
        }
        return ResponseEntity.badRequest().body((Map.of("Error", reply.message())));
    }
}
