package com.ordersystem.mapper;

import com.ordersystem.dto.ProductDto;
import com.ordersystem.entity.Product;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring")
public interface ProductMapper {

    @Mapping(target = "availableQuantity", ignore = true)
    ProductDto toDto(Product product);

    Product toEntity(ProductDto dto);

    default ProductDto toDtoWithInventory(Product product, Integer availableQuantity) {
        ProductDto dto = toDto(product);
        dto.setAvailableQuantity(availableQuantity);
        return dto;
    }
}
