package com.example.demo.kafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

@Configuration
public class KafkaConsumerConfig {
	

	private ConsumerFactory<String,Object> consumerFactory;
	
	
	public KafkaConsumerConfig(ConsumerFactory<String, Object> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}


	@Bean
	KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>>kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory);
		factory.setConcurrency(6);
		factory.setBatchListener(true);
		factory.getContainerProperties().setPollTimeout(3000);
		//factory.setCommonErrorHandler(new DefaultErrorHandler());
		return factory;
	}


}
