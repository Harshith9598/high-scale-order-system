package com.ordersystem.mapper;

import com.ordersystem.dto.OrderDto;
import com.ordersystem.entity.Order;
import com.ordersystem.entity.OrderItem;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

import java.util.List;

@Mapper(componentModel = "spring")
public interface OrderMapper {

    @Mapping(source = "user.id", target = "userId")
    OrderDto toDto(Order order);

    @Mapping(source = "product.id", target = "productId")
    @Mapping(source = "product.name", target = "productName")
    OrderDto.OrderItemDto toItemDto(OrderItem item);

    List<OrderDto.OrderItemDto> toItemDtoList(List<OrderItem> items);

    default OrderDto toDtoWithItems(Order order) {
        OrderDto dto = toDto(order);
        if (order.getItems() != null) {
            dto.setItems(toItemDtoList(order.getItems()));
        }
        return dto;
    }
}
