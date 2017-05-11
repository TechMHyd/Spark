package com.techmahindra.vehicletelemetry.service;

import java.io.IOException;
import java.io.Serializable;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.techmahindra.vehicletelemetry.utils.CarEventProducer;

public class MaintenanceAnalyzerService implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private static final String OUT_TOPIC = "caralerts";
	
	public void process(Dataset<Row> tempData) {
		if(tempData != null && tempData.count() > 0) {
			tempData.foreach(new ForeachFunction<Row>() {
				private static final long serialVersionUID = 1L;
				@Override
				public void call(Row row) throws Exception {
					String vin = row.getString(row.fieldIndex("vin"));
					String city = row.getString(row.fieldIndex("city"));
					String model = row.getString(row.fieldIndex("model"));
					Double aot = row.getDouble(row.fieldIndex("avg_outtemp"));
					Double aet = row.getDouble(row.fieldIndex("avg_enginetemp"));
					boolean cond1 = aot > 90 && aet > 250;
					boolean cond2 = aot > 60 && aot < 90 && aet > 200;
					boolean cond3 = aot < 60 && aet > 150; 
					if((cond1) || (cond2) || (cond3)) {
						generateAlert(vin, city, model);
					}
				}
			});
		}
	}
	
	private void generateAlert(String vin, String city, String model) throws IOException {
		String msg="{\"vin\":\""+ vin +"\","
				  + "\"city\":\"" + city + "\","
				  + "\"model\":\"" + model + "\","
				  + "\"maintenance\":\"Y\"}";
		CarEventProducer cep = new CarEventProducer();
		cep.sendEvent(OUT_TOPIC, msg);
	}
	//git test 
}
