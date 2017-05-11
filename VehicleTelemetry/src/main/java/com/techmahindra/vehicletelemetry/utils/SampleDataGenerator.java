package com.techmahindra.vehicletelemetry.utils;

import java.io.IOException;

import com.google.gson.Gson;
import com.techmahindra.vehicletelemetry.vo.CarEvent;

public class SampleDataGenerator {
	
	public void generate() throws IOException {
		CarEventProducer cep = new CarEventProducer();
		CarEvent event = new CarEvent();
		Gson gson = new Gson();
    	String msg = gson.toJson(event);
		cep.sendEvent("carevents", msg);
		msg="{\"vin\":\"DUMMY\",\"model\":\"DUMMY\",\"city\":\"DUMMY\",\"maintenance\":\"Y\"}";
		cep.sendEvent("caralerts", msg);
	}
	
	public static void main(String[] args) throws IOException {
		(new SampleDataGenerator()).generate();
	}

}
