package com.benja.course.springcloud.kafka.command.services;

import com.benja.course.springcloud.kafka.command.model.dto.ProductDto;

import java.util.List;

public interface ProductService {
    ProductDto createProduct(ProductDto dto);
    ProductDto findById(Long id);
    List<ProductDto> findAll();
}
