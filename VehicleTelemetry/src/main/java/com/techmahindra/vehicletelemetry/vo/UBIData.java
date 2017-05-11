package com.techmahindra.vehicletelemetry.vo;

public class UBIData {

	private String vin;
	private String date;
	private int driverScore;
	private float driverScorePerc;
	private float discount;
	
	
	public UBIData() {
		super();
		// TODO Auto-generated constructor stub
	}

	public UBIData(String vin, String date, int driverScore,
			float driverScorePerc, float discount) {
		super();
		this.vin = vin;
		this.date = date;
		this.driverScore = driverScore;
		this.driverScorePerc = driverScorePerc;
		this.discount = discount;
	}
	
	public String getVin() {
		return vin;
	}
	public void setVin(String vin) {
		this.vin = vin;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public int getDriverScore() {
		return driverScore;
	}
	public void setDriverScore(int driverScore) {
		this.driverScore = driverScore;
	}
	public float getDriverScorePerc() {
		return driverScorePerc;
	}
	public void setDriverScorePerc(float driverScorePerc) {
		this.driverScorePerc = driverScorePerc;
	}
	public float getDiscount() {
		return discount;
	}
	public void setDiscount(float discount) {
		this.discount = discount;
	}
	
	
	
	
}
