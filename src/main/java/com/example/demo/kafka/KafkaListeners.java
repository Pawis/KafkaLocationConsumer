package com.example.demo.kafka;


import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.ListenerUtils;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.stereotype.Service;

import com.example.demo.mapper.LocationMapper;
import com.example.demo.model.dto.LocationDto;
import com.example.demo.repository.LocationRepository;

@Service
public class KafkaListeners {

	private static final Logger log = LoggerFactory.getLogger(KafkaListener.class);

	private LocationMapper mapstructMapper;	
	private LocationRepository locationRepo;


	public KafkaListeners(LocationRepository locationRepo,LocationMapper mapstructMapper) {
		this.locationRepo = locationRepo;
		this.mapstructMapper = mapstructMapper;
	}

	@KafkaListener(topics = "locations")
	public void listener1(List<ConsumerRecord<String,LocationDto>> location) {
		
		for(ConsumerRecord<String,LocationDto> data : location) {

			if(data.value() == null) {
				DeserializationException deserEx = ListenerUtils.getExceptionFromHeader(data,
						SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER,new LogAccessor(KafkaListeners.class));
				if(deserEx != null) {
					log.error("Record at offset " + data.offset() + " could not be deserialized",deserEx);
					throw new BatchListenerFailedException("Deserialization", deserEx, 1);
				}

			}

			log.info("Recived locations from kafka, sending to DB...");
			log.info("Batch size: " + location.size());
			log.info("Recived: " + data.value().toString() );
			log.info("Topic: " + data.topic());
			log.info("Partition: " + data.partition());
			locationRepo.save(mapstructMapper.dtoToLocation(data.value()));
		}

	}

}
