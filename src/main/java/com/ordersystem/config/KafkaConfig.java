package com.ordersystem.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.topics.order-created}")
    private String orderCreatedTopic;

    @Value("${app.kafka.topics.inventory-reserved}")
    private String inventoryReservedTopic;

    @Value("${app.kafka.topics.inventory-failed}")
    private String inventoryFailedTopic;

    @Value("${app.kafka.topics.payment-processed}")
    private String paymentProcessedTopic;

    @Value("${app.kafka.topics.payment-failed}")
    private String paymentFailedTopic;

    @Value("${app.kafka.topics.order-completed}")
    private String orderCompletedTopic;

    @Value("${app.kafka.topics.notification-created}")
    private String notificationCreatedTopic;

    @Value("${app.kafka.topics.order-created-dlt}")
    private String orderCreatedDlt;

    @Value("${app.kafka.topics.inventory-reserved-dlt}")
    private String inventoryReservedDlt;

    @Value("${app.kafka.topics.payment-processed-dlt}")
    private String paymentProcessedDlt;

    @Value("${app.kafka.topics.notification-created-dlt}")
    private String notificationCreatedDlt;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(config);
        JsonSerializer<Object> serializer = new JsonSerializer<>(mapper);
        serializer.setAddTypeInfo(true); // __TypeId__ header: consumers deserialize to the concrete event class
        factory.setValueSerializer(serializer);
        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "order-platform-group");
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        // Honor the producer's __TypeId__ header so each @Payload arrives as its
        // concrete event class. Previously this was JsonDeserializer(Object,
        // useHeaders=false) with a single VALUE_DEFAULT_TYPE for every topic,
        // so listeners received Maps instead of events.
        JsonDeserializer<Object> deserializer = new JsonDeserializer<>(Object.class, mapper, true);
        deserializer.addTrustedPackages("com.ordersystem.kafka.event");

        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            org.springframework.kafka.listener.CommonErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // RECORD: the container commits the offset after the listener returns,
        // i.e. after its DB transaction has committed. (The old MANUAL_IMMEDIATE
        // ack() inside @Transactional listeners committed offsets before the DB.)
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        factory.setConcurrency(3);
        return factory;
    }

    // Topics
    @Bean
    public NewTopic orderCreatedTopic() {
        return TopicBuilder.name(orderCreatedTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryReservedTopic() {
        return TopicBuilder.name(inventoryReservedTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryFailedTopic() {
        return TopicBuilder.name(inventoryFailedTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic paymentProcessedTopic() {
        return TopicBuilder.name(paymentProcessedTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic paymentFailedTopic() {
        return TopicBuilder.name(paymentFailedTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic orderCompletedTopic() {
        return TopicBuilder.name(orderCompletedTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic notificationCreatedTopic() {
        return TopicBuilder.name(notificationCreatedTopic).partitions(3).replicas(1).build();
    }

    // DLT Topics
    @Bean
    public NewTopic orderCreatedDltTopic() {
        return TopicBuilder.name(orderCreatedDlt).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryReservedDltTopic() {
        return TopicBuilder.name(inventoryReservedDlt).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic paymentProcessedDltTopic() {
        return TopicBuilder.name(paymentProcessedDlt).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic notificationCreatedDltTopic() {
        return TopicBuilder.name(notificationCreatedDlt).partitions(1).replicas(1).build();
    }

    // DLT topics for the remaining consumed topics (error handler routes
    // <topic>.DLT; replicas=1 matches the single-broker local setup).
    @Bean
    public NewTopic inventoryFailedDltTopic() {
        return TopicBuilder.name(inventoryFailedTopic + ".DLT").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic paymentFailedDltTopic() {
        return TopicBuilder.name(paymentFailedTopic + ".DLT").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic orderCompletedDltTopic() {
        return TopicBuilder.name(orderCompletedTopic + ".DLT").partitions(1).replicas(1).build();
    }
}
