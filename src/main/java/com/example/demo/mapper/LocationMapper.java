package com.example.demo.mapper;

import org.mapstruct.Mapper;

import com.example.demo.model.dto.LocationDto;
import com.example.demo.model.entity.Location;

@Mapper(componentModel="spring")
public interface LocationMapper {
	
	
	Location dtoToLocation(LocationDto location);


}
