package com.techmahindra.vehicletelemetry.service;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.Iterator;

import com.techmahindra.vehicletelemetry.utils.CarEventProducer;
import com.techmahindra.vehicletelemetry.utils.DumpCarEventsData;
import com.techmahindra.vehicletelemetry.utils.UBIUtils;
import com.techmahindra.vehicletelemetry.vo.UBIData;

public class UBIAnalyzerService implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger("UBIAnalyzerService");
	private static final String OUT_TOPIC = "caralerts";
	
	
	public List<UBIData> process(Dataset<Row> tempData) {
		List <UBIData>ubiDataList=new ArrayList<>();
		
		if(tempData != null /*&& tempData.count() > 0*/) {
			
			//List<Row> rows=tempData.collectAsList();
			RDD<Row> rows=tempData.rdd();
			/*tempData.filter(new FilterFunction<Row>(){
			});*/
			Iterator<Row> iter=rows.toLocalIterator();
			while(iter.hasNext())
			{
			Row row=iter.next();	
			//for(Row row:rows){
			/*tempData.foreach(new ForeachFunction<Row>() {
				private static final long serialVersionUID = 1L;
				@Override
				public void call(Row row) throws Exception {*/
					String vin = row.getString(row.fieldIndex("vin"));
					String date = row.getString(row.fieldIndex("date"));
					float totalTrips = Float.valueOf(row.getLong(row.fieldIndex("trips")));
					float tripsHavingOS = Float.valueOf(row.getLong(row.fieldIndex("OSCount")));
					float tripsHavingHB = Float.valueOf(row.getLong(row.fieldIndex("HBCount")));
					float tripsHavingHC = Float.valueOf(row.getLong(row.fieldIndex("HCCount")));
					float tripsHavingHA = Float.valueOf(row.getLong(row.fieldIndex("HACount")));
					float noOfTripsNight = Float.valueOf(row.getLong(row.fieldIndex("NightTripCount")));
					float noOfTripsWeekend = Float.valueOf(row.getLong(row.fieldIndex("WekendTripCount")));
					if((tripsHavingHB+tripsHavingHA+tripsHavingHC+tripsHavingOS) > totalTrips)
					{
						System.out.println("There is a data issue at database, please re-check total trips and reletive counts of vin :"+vin
								+". on ("+date+").Ignoring the record !!! ");
					}else{

						float driverScorePerc=getDriverScorePercentage(totalTrips, tripsHavingHB, tripsHavingHC, tripsHavingHA, tripsHavingOS);
						int driverScore=getDriverScore(driverScorePerc);

						float discount=getDiscount(totalTrips, tripsHavingHB, tripsHavingHC, tripsHavingHA, tripsHavingOS, noOfTripsNight, noOfTripsWeekend);
						ubiDataList.add(new UBIData(vin,date,driverScore,driverScorePerc,discount));
						/*if("Y".equalsIgnoreCase(fPersistProcessData))
						{
							persistUBIData(vin,date,driverScore,driverScorePerc,discount);
						}*/
					}

				}
			//});
		}else
		{
			logger.info("No aggregate data to process!");
		}
		return ubiDataList;
	}
	
	
	public static int getDriverScore(float driverScoreInPercentage)
	{
		//IF(C40>80%,5,IF(C40>60%,4,IF(C40>40%,3,IF(C40>20%,2,1))))
		if(driverScoreInPercentage>UBIUtils.getSCORE_PERC_GT80()){
			return (UBIUtils.getSCORE_5());
		}else if(driverScoreInPercentage>UBIUtils.getSCORE_PERC_GT60()){
			return (UBIUtils.getSCORE_4());
		}else if(driverScoreInPercentage>UBIUtils.getSCORE_PERC_GT40()){
			return (UBIUtils.getSCORE_3());
		}else if(driverScoreInPercentage>UBIUtils.getSCORE_PERC_GT20()){
			return (UBIUtils.getSCORE_2());
		}else
			return (UBIUtils.getSCORE_1());

	}

	public static float getDriverScorePercentage(float totalTrips,float tripsHavingHB,float tripsHavingHC,float tripsHavingHA,float tripsHavingOS)
	{
		//=OS*E29+HA*E27+HC*E28+HB*E26
		//		OS%	16.00%
		//		HA%	32.00%
		//		HC%	44.00%
		//		HB%	8.00%
		float confOSPerc=UBIUtils.getOS_PERC()/100f;//read it from property
		float confHAPerc=UBIUtils.getHA_PERC()/100f;
		float confHCPerc=UBIUtils.getHC_PERC()/100f;
		float confHBPerc=UBIUtils.getHB_PERC()/100f;

		float derivedOSPerc=((totalTrips-tripsHavingOS)/totalTrips)*100f;
		float derivedHAPerc=((totalTrips-tripsHavingHA)/totalTrips)*100f;
		float derivedHCPerc=((totalTrips-tripsHavingHC)/totalTrips)*100f;
		float derivedHBPerc=((totalTrips-tripsHavingHB)/totalTrips)*100f;

		return (confOSPerc*derivedOSPerc+confHAPerc*derivedHAPerc+confHBPerc*derivedHBPerc+confHCPerc*derivedHCPerc);

	}

	public static float getDiscountPercentage(float totalTrips,float noOfTripsNight,float noOfTripsWeekend, float driverScorePercentage)
	{
		/*Max Disc %	30%
		NDB%	20%
		DB%	80%
		TOD%	80%
		DOW%	20%*/
		
		float maxDiscount=UBIUtils.getMAX_DISC()/100f;//get it from property
		float maxNDB=UBIUtils.getMAX_NDB()/100f;
		float maxDB=UBIUtils.getMAX_DB()/100f;
		float maxTOD=UBIUtils.getMAX_TOD()/100f;
		float maxDOW=UBIUtils.getMAX_DOW()/100f;
		
		float derivedNoOfTripsNight=((totalTrips-noOfTripsNight)/totalTrips)*100f;
		float derivedNoOfTripsWeekend=((totalTrips-noOfTripsWeekend)/totalTrips)*100f;
		
		float discount=(maxDiscount*(maxNDB*((maxTOD*derivedNoOfTripsNight)+(maxDOW*derivedNoOfTripsWeekend))+(maxDB*driverScorePercentage)));
		return discount;
	}
	
	public static float getDiscount(float totalTrips,float tripsHavingHB,float tripsHavingHC,float tripsHavingHA,float tripsHavingOS,float noOfTripsNight,float noOfTripsWeekend)
	{
		float driverScorePercentage=getDriverScorePercentage(totalTrips, tripsHavingHB, tripsHavingHC, tripsHavingHA, tripsHavingOS);
		return getDiscountPercentage(totalTrips,noOfTripsNight, noOfTripsWeekend, driverScorePercentage);
	}
	
	public static void main(String[] args) {
		
		UBIAnalyzerService service=new UBIAnalyzerService();
		/*float totalTrip=5;
		float tripHavingHB=2;
		
		float score=((totalTrip-tripHavingHB)/totalTrip)*100;
		System.out.println("score:"+score);
		float perc=(score*100/100);
		System.out.println("HB perc:"+perc);
		
		float confOSPerc=(16*100/100);//read it from property
		float confHAPerc=(32*100)/100;
		System.out.println("confOSPerc:"+confOSPerc +" confHAPerc:"+confHAPerc);*/
		
		//System.out.println("res:"+(float) 30 / 100f);
		
		System.out.println("Result:"+(0.30*(0.20*(0.80*100+0.20*100)+0.80*79.2)));
		
		float totalTrips=5;
		float tripsHavingHB=2;
		float tripsHavingHC=2;
		float tripsHavingHA=0;
		float tripsHavingOS=0;
		float noOfTripsNight=0;
		float noOfTripsWeekend=0;
		
		float driverScorePerc=service.getDriverScorePercentage(totalTrips, tripsHavingHB, tripsHavingHC, tripsHavingHA, tripsHavingOS);
		System.out.println("driverScorePerc:"+driverScorePerc);
		float discount=service.getDiscount(totalTrips, tripsHavingHB, tripsHavingHC, tripsHavingHA, tripsHavingOS, noOfTripsNight, noOfTripsWeekend);
		
		System.out.println("discount offered:"+discount);
		
		totalTrips=5;
		tripsHavingHB=2;
		tripsHavingHC=2;
		tripsHavingHA=1;
		tripsHavingOS=0;
		noOfTripsNight=0;
		noOfTripsWeekend=0;
		
		driverScorePerc=service.getDriverScorePercentage(totalTrips, tripsHavingHB, tripsHavingHC, tripsHavingHA, tripsHavingOS);
		System.out.println("driverScorePerc:"+driverScorePerc);
		discount=service.getDiscount(totalTrips, tripsHavingHB, tripsHavingHC, tripsHavingHA, tripsHavingOS, noOfTripsNight, noOfTripsWeekend);
		System.out.println("discount offered again:"+discount);
		
		
	}
	
	private void persistUBIData(String vin, String date,int driverScore, float driverScorePerc, float discount) throws SQLException {
		DumpCarEventsData.persistUBIData(vin, date,driverScore, driverScorePerc, discount);
		
	}
	private void generateAlert(String vin, String city, String model) throws IOException {
		String msg="{\"vin\":\""+ vin +"\","
				  + "\"city\":\"" + city + "\","
				  + "\"model\":\"" + model + "\","
				  + "\"maintenance\":\"Y\"}";
		CarEventProducer cep = new CarEventProducer();
		cep.sendEvent(OUT_TOPIC, msg);
	}


	public void processIt(Dataset<Row> ubiAggregateData) 
	{
		Dataset<Row> data=ubiAggregateData.unpersist();
		data.show();
	}
}
