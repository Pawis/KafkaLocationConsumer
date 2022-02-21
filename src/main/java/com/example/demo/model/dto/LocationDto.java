package com.example.demo.model.dto;


public class LocationDto {

	private String deviceId;
	private int latitiude;
	private int longitude;

	public LocationDto(String deviceId, int latitiude, int longitude) {
		this.deviceId = deviceId;
		this.latitiude = latitiude;
		this.longitude = longitude;
	}


	public LocationDto() {
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
		return "LocationDto [deviceId=" + deviceId + ", latitiude=" + latitiude + ", longitude=" + longitude + "]";
	}
	
	


}
