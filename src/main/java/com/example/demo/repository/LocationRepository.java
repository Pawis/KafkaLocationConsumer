package com.example.demo.repository;

import org.springframework.data.repository.CrudRepository;

import com.example.demo.model.entity.Location;

public interface LocationRepository extends CrudRepository<Location,String>{

	
}
