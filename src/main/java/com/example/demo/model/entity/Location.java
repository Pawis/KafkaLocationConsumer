package com.example.demo.model.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Location {

	@Id
	@Column(name= "deviceid")
	private String deviceId;
	@Column(name= "latitiude")
	private int latitiude;
	@Column(name= "longitude")
	private int longitude;

	public Location(String deviceId, int latitiude, int longitude) {
		this.deviceId = deviceId;
		this.latitiude = latitiude;
		this.longitude = longitude;
	}


	public Location() {
		super();
	}



	public String getDeviceId() {
		return deviceId;
	}



	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}



	public int getLatitiude() {
		return latitiude;
	}



	public void setLatitiude(int latitiude) {
		this.latitiude = latitiude;
	}



	public int getLongitude() {
		return longitude;
	}



	public void setLongitude(int longitude) {
		this.longitude = longitude;
	}



	@Override
	public String toString() {
		return "Location [deviceId=" + deviceId + ", latitude=" + latitiude + ", longitude=" + longitude + "]";
	}
	
	



}
