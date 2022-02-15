package com.example.demo.serializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.demo.model.dto.LocationDto;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LocationDeserializer implements Deserializer<LocationDto> {
	
	private static final Logger log = LoggerFactory.getLogger(LocationDeserializer.class);

	public final ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public LocationDto deserialize(String topic, byte[] data) {

		try {
			if(data == null) {
				log.warn("Trying to deserialize null data ");
				return null;
			}
			log.debug("Deserializing...");
			return objectMapper.readValue(data, LocationDto.class);

		}catch (Exception e) {
			throw new SerializationException("Error when deserializing byte[] to LocationDto");
		}

	}

}
