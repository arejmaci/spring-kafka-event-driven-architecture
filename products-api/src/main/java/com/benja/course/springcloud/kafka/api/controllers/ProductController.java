package com.benja.course.springcloud.kafka.api.controllers;

import com.benja.course.springcloud.kafka.api.model.dto.ProductDto;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.benja.course.springcloud.kafka.api.services.ProductCommandService;

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
     * Recibe los datos de un producto y solicita su creación.
     *
     * @param productDto datos validados del producto
     * @return confirmación de que el comando de creación fue enviado
     */
    @PostMapping
    public ResponseEntity<?> createProduct(@Valid @RequestBody ProductDto productDto) {
        productCommandService.sendCreate(productDto);
        return ResponseEntity.ok().body(Map.of("message", "Product creation command sent successfully"));
    }
}
