package com.benja.course.springcloud.kafka.command.services;

import com.benja.course.springcloud.kafka.command.entities.Product;
import com.benja.course.springcloud.kafka.command.model.dto.ProductDto;
import com.benja.course.springcloud.kafka.command.model.mapper.Mappers;
import com.benja.course.springcloud.kafka.command.repositories.ProductRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;

import static com.benja.course.springcloud.kafka.command.model.mapper.Mappers.toDto;

@Service
public class ProductServiceImp implements ProductService {

    private final ProductRepository productRepository;

    public ProductServiceImp(ProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    @Override
    @Transactional
    public ProductDto createProduct(ProductDto dto) {
        Product product = productRepository.save(Mappers.toEntity(dto));
        return toDto(product);
    }

    @Override
    @Transactional(readOnly = true)
    public ProductDto findById(Long id){
        return productRepository.findById(id).map(Mappers::toDto).orElse(null);
    }

    @Override
    public List<ProductDto> findAll() {
        return productRepository.findAll().stream().map(Mappers::toDto).toList();
    }

    @Override
    public ProductDto updateProduct(ProductDto dto, Long id) {
        Product product = productRepository.findById(id).orElse(null);
        if(product == null ){
            return null;
        }
        product.setName(dto.name());
        product.setPrice(dto.price());
        return Mappers.toDto(productRepository.save(product));
    }
}
