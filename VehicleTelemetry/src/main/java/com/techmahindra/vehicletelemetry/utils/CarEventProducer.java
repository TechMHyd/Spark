package com.techmahindra.vehicletelemetry.utils;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.techmahindra.vehicletelemetry.vo.CarEvent;

public class CarEventProducer {
	
	private Producer<String, String> producer;
	private static Logger logger = LoggerFactory.getLogger("CarEventProducer");
	
	public CarEventProducer() throws IOException {
		Properties props = new Properties();
		props.load(this.getClass().getClassLoader().getResourceAsStream("vt.properties"));
		// Prepare kafka properties
		Properties kafkaProps = new Properties();
		kafkaProps.put("metadata.broker.list", props.getProperty("KAFKA_BROKER_LIST"));
        kafkaProps.put("serializer.class", "kafka.serializer.StringEncoder");
        kafkaProps.put("request.required.acks", "1");
        // Instantiate producer
        ProducerConfig config = new ProducerConfig(kafkaProps);
        producer = new Producer<>(config);
	}
	//.\bin\spark-submit.cmd --class com.techmahindra.vehicletelemetry.CarEventProcessor --master local[4] C:\Users\admin\.m2\repository\com\techmahindra\vehicletelemetry\0.0.1\vehicletelemetry-0.0.1-jar-with-dependencies.jar carevents
	
	public static void main(String[] args) throws IOException {

		
		String filePath="D:/SparkWorks/CarEventsData/UBI_Data_Modified/rawcareventstream/";
		
		// Generate events using file
		CarEventProducer cdp = new CarEventProducer();
		if(args.length ==0){
			logger.info("No args passed considering default one !!!");
			System.out.println(System.getenv("os"));
			System.out.println(System.getenv());
			String fileSep=File.separator;
			String userHome=System.getProperty("user.home");
			System.out.println("userHome:"+userHome);
			
			//args=new String[]{"carevents",userHome+fileSep+"sample_data"+fileSep+"vt"+fileSep+"current"+fileSep+"file_2.csv"};
			 //args=new String[]{"carevents","D:/SparkWorks/CarEventsData/UBI_Data_Modified/rawcarevents/file_1.csv"};
			//carevents
			//args=new String[]{"carevents",filePath+"824507016_30dc7f8019f24402a2df1a49c21689bf_2015-03-30.csv"};
			//args=new String[]{"carevents",filePath+"824507016_30dc7f8019f24402a2df1a49c21689bf_2015-04-27_28_29_30.csv"};
			//args=new String[]{"carevents",filePath+"824507016_30dc7f8019f24402a2df1a49c21689bf_2015-04-27.csv"};
			//args=new String[]{"carevents",filePath+"824507016_30dc7f8019f24402a2df1a49c21689bf_2015-04-28.csv"};
			//args=new String[]{"carevents",filePath+"824507016_30dc7f8019f24402a2df1a49c21689bf_2015-04-29.csv"};
			//args=new String[]{"carevents",filePath+"824507016_30dc7f8019f24402a2df1a49c21689bf_2015-04-30.csv"};
			//args=new String[]{"carevents",filePath+"824507016_30dc7f8019f24402a2df1a49c21689bf_2015-04-31.csv"};
			//ubievents
			 //args=new String[]{"ubievents","D:/SparkWorks/CarEventsData/UBI_Data_Modified/rawcarevents/file_1.csv"};
			//args=new String[]{"ubievents",filePath+"824507016_30dc7f8019f24402a2df1a49c21689bf_2015-03-30.csv"};
			//args=new String[]{"ubievents",filePath+"824507016_30dc7f8019f24402a2df1a49c21689bf_2015-04-27_28_29_30.csv"};
			args=new String[]{"ubievents",filePath+"824507016_30dc7f8019f24402a2df1a49c21689bf_2015-04-27.csv"};
			//args=new String[]{"ubievents",filePath+"824507016_30dc7f8019f24402a2df1a49c21689bf_2015-04-28.csv"};
			//args=new String[]{"ubievents",filePath+"824507016_30dc7f8019f24402a2df1a49c21689bf_2015-04-29.csv"};
			//args=new String[]{"ubievents",filePath+"824507016_30dc7f8019f24402a2df1a49c21689bf_2015-04-30.csv"};
			//args=new String[]{"ubievents",filePath+"824507016_30dc7f8019f24402a2df1a49c21689bf_2015-04-31.csv"};
			
			
		}
		if(args.length<1)
		{
			logger.error("Usage: CarDataProducer <csvFilePathToRead> <topic>");
			System.exit(1);
		}
		//cdp.generateCarEvents(10);
		cdp.eventsFromFile(args[0], args[1]);
	}
	
	public void sendEvent(String topic, String msg) {
		// Send event to topic
		KeyedMessage<String, String> data = new KeyedMessage<>(topic, msg);
		//System.out.println("Message :"+msg);
		producer.send(data);
	}
	
	private void eventsFromFile(String topic, String file) throws IOException {
		// Read a file and generate events
		int msgCount = -1;
        try (Scanner scanner = new Scanner(new File(file))) {
	        logger.info("Sending messages...");
	        //System.out.println("Sending messages...");
	        while(scanner.hasNextLine()) {
	        	msgCount++;
	        	String line = scanner.nextLine();
				if(msgCount > 0) {
		        	CarEvent event = parse(line);
		        	Gson gson = new Gson();
		        	String msg = gson.toJson(event);
					sendEvent(topic, msg);
				}
				pause();
			}
        } catch(IOException e) {
        	logger.error("IOE", e);
        	e.printStackTrace();
        	throw e;
        }
        logger.info("Sent " + msgCount + " messages");
        //System.out.println("Sent " + msgCount + " messages");
	}
	
	private void pause() {
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			logger.error("IE", e);
			Thread.currentThread().interrupt();
		}
	}
	
	private CarEvent parse(String record) {
		// Parse lines of file into car event
		CarEvent event = new CarEvent();
		String[] fields = record.split(",");
		event.setVin(fields[0]);
		event.setModel(fields[1]);
		event.setTimestamp(fields[2]);
		event.setOutsideTemp(Float.parseFloat(fields[3]));
		event.setEngineTemp(Float.parseFloat(fields[4]));
		event.setSpeed(Float.parseFloat(fields[5]));
		event.setFuel(Float.parseFloat(fields[6]));
		event.setEngineOil(Float.parseFloat(fields[7]));
		event.setTirePressure(Float.parseFloat(fields[8]));
		event.setOdometer(Long.parseLong(fields[9]));
		event.setCity(fields[10]);
		event.setAccPedalPos(Integer.parseInt(fields[11]));
		event.setParkBrakeStatus(Integer.parseInt(fields[12]));
		event.setHeadlampStatus(Integer.parseInt(fields[13]));
		event.setBrakePedalStatus(Integer.parseInt(fields[14]));
		event.setTransGearPosition(fields[15]);
		event.setIgnitionStatus(Integer.parseInt(fields[16]));
		event.setWindshieldWiperStatus(Integer.parseInt(fields[17]));
		event.setAbs(Integer.parseInt(fields[18]));
		event.setTripId(fields[19]);
		event.setIncidentType(fields[20]);
		return event;
	}
	
	
	private String[] getVins() {
		
		String[] vins=new String[]{
				"SECR0ECMYSH0YD1YA", "KFADKS5LP01N0Z7TW","KEB3UH45SYSONXL1Q","JSUOVVJFH8D183GCK","186QWIKBA7T5EYP2U",
				"0MKU5TFQWZO9HS11S", "9GSW2BOQQ3HKVJZZU","50EIKRNDAJN5UUJ4P","Q0GRV0ZZYK2TKGFW2","ORSUL9LZY92OIIR6L",
				"RC845HQH9LK2QALR1", "FUGSPCGFPW9L66ZWC","IA1N3EEX2BR2DD5HM","ZOPYIRVU7EI9GHUMQ","3HWY9QOTM78TUKUD8",
				"70B85HK639ITWWRV2", "75VT86IS1UV82VDDN","PGGCTO2VLSWPW8IIM","7X5AAFGEPWLZ10PQ6","KOXX7GFJ79EEJVF28",
				"QFD6C720QLI4JL5YJ", "6HJMCXBYWFJXUX7IM","VLPNYAXKVRK7HPOF8","C27GX6BM2GYV9HX9Y","E1U7K2VBNE5SZ0785",
				"QKZ2GA81V27UM0E5M", "4XBKFGGH188UOJQVV","TWSYNJXQ4PP3WXXM9","D9NR4FLZD8HYSZHEP","0EMM2KMRA875ZZRNG",
				"B4K1B6IHQG3NSA7B7", "AQTB6Y67HX3106CE2","JCK37G5T3HEBN0M33"/*,getVins()[new Random().nextInt()]*/
		};
	return vins;
	}
	
	
	private String[] getTimeStampsMorning() {
		
		String[] tStamps=new String[]{
				"2015-04-28T08:06:22.6613426Z", "2015-04-28T08:12:22.6211426Z","2015-04-28T08:38:22.6613426Z",
				"2015-04-28T09:39:22.6613426Z", "2015-04-28T09:42:22.6211426Z","2015-04-28T09:48:22.6613426Z",
				"2015-04-28T10:31:22.6613426Z", "2015-04-28T11:42:22.6211426Z","2015-04-28T11:49:22.6613426Z",
		};
	return tStamps;
	}
	
	
	private String[] getTimeStampsMidDay() {
		
		String[] tStamps=new String[]{
				"2015-04-28T12:12:22.6613426Z","2015-04-28T12:25:22.6613426Z","2015-04-28T13:20:22.6613426Z",
				"2015-04-28T12:16:22.6613426Z","2015-04-28T12:35:22.6613426Z","2015-04-28T13:30:22.6613426Z",
				"2015-04-28T12:29:22.6613426Z","2015-04-28T12:45:22.6613426Z","2015-04-28T13:45:22.6613426Z",
				"2015-04-28T12:33:22.6613426Z","2015-04-28T12:55:22.6613426Z","2015-04-28T13:50:22.6613426Z",
				"2015-04-28T14:32:22.6613426Z","2015-04-28T14:25:22.6613426Z","2015-04-28T14:50:22.6613426Z",
				"2015-04-28T14:31:22.6613426Z","2015-04-28T14:44:22.6613426Z","2015-04-28T14:55:22.6613426Z",
		};
	return tStamps;
	}

private String[] getTimeStampsEvening() {
		
		String[] tStamps=new String[]{
				"2015-04-28T16:32:22.6613426Z","2015-04-28T17:24:22.6613426Z","2015-04-28T18:13:22.6613426Z",
				"2015-04-28T16:39:22.6613426Z","2015-04-28T17:30:22.6613426Z","2015-04-28T18:27:22.6613426Z",
				"2015-04-28T16:49:22.6613426Z","2015-04-28T17:40:22.6613426Z","2015-04-28T18:29:22.6613426Z",
				"2015-04-28T16:59:22.6613426Z","2015-04-28T17:45:22.6613426Z","2015-04-28T18:42:22.6613426Z",
		};
	return tStamps;
	}

private String[] getTimeStampsNight() {
	
	String[] tStamps=new String[]{
			"2015-04-28T19:39:22.6613426Z","2015-04-28T19:30:22.6613426Z","2015-04-28T19:28:22.6613426Z",
			"2015-04-28T20:39:22.6613426Z","2015-04-28T20:30:22.6613426Z","2015-04-28T20:28:22.6613426Z",
			"2015-04-28T21:39:22.6613426Z","2015-04-28T21:30:22.6613426Z","2015-04-28T22:28:22.6613426Z",
	};
return tStamps;
}

private String[] getModels() {
	String[] models=new String[]{
			"Saloon","Medium SUV","Hybrid","Sedan",
			"Sports Car","Large SUV","Convertible","Coupe",
			"Compact car","Small SUV","Family Saloon"
	};
	return models;
}

private String[] getCities() {
	String[] models=new String[]{
			"Seattle","Sammamish","Redmond","Bellevue",

	};
	return models;
}

private String[] getIncTypes() {
	String[] models=new String[]{
			"HardBraking","HardAcceleration","OverSpeed","HardCornering",
	};
	return models;
}

	
	
	public void generateCarEvents(int reqRecords)
	{
		StringBuffer sb=new StringBuffer();
		
		String csvHeader="vin,model,timestamp,outsidetemperature,enginetemperature,speed,fuel,engineoil,tirepressure,odometer,city,accelerator_pedal_position,parking_brake_status,headlamp_status,brake_pedal_status,transmission_gear_position,ignition_status,windshield_wiper_status,abs,TripID,IncidentType";
		sb.append(csvHeader);
		
		for(int recCntr=0;recCntr<=reqRecords;recCntr++){
			String vin=generateRandomString();
			for(int i=0;i<getCities().length;i++)
			{
				String city=getCities()[i];
				for(int j=0;j<getModels().length;j++)
				{
					String model=getModels()[j];
					String nextData="91,233,76,9,31,7,186061,";
					String nextData2="2,0,0,0,fourth,0,0,0,";
					
					for(int k=0;k<new Random().nextInt(getTimeStampsMorning().length);k++)
					{
						String csvLine=vin+","+model+","+getTimeStampsMorning()[k]+","+nextData+city+","
					     +nextData2+vin+"_"+(getTimeStampsMorning()[k]).substring(0, 19)+","+getIncTypes()[new Random().nextInt(4)];
						sb.append("\n"+csvLine);
						//System.out.println(csvLine);
					}
					for(int k=0;k<new Random().nextInt(getTimeStampsMidDay().length);k++)
					{
						String csvLine=vin+","+model+","+getTimeStampsMidDay()[k]+","+nextData+city+","
							     +nextData2+vin+"_"+(getTimeStampsMidDay()[k]).substring(0, 19)+","+getIncTypes()[new Random().nextInt(4)];
						sb.append("\n"+csvLine);
					}
					for(int k=0;k<new Random().nextInt(getTimeStampsEvening().length);k++)
					{
						String csvLine=vin+","+model+","+getTimeStampsEvening()[k]+","+nextData+city+","
							     +nextData2+vin+"_"+(getTimeStampsEvening()[k]).substring(0, 19)+","+getIncTypes()[new Random().nextInt(4)];
						sb.append("\n"+csvLine);
					}
					for(int k=0;k<new Random().nextInt(getTimeStampsNight().length);k++)
					{
						String csvLine=vin+","+model+","+getTimeStampsNight()[k]+","+nextData+city+","
							     +nextData2+vin+"_"+(getTimeStampsNight()[k]).substring(0, 19)+","+getIncTypes()[new Random().nextInt(4)];
						sb.append("\n"+csvLine);
					}
				}

			}
		}
		System.out.println(sb.toString());
	}
	
	private static final String CHAR_LIST =  "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
	    private static final int RANDOM_STRING_LENGTH = 17;
	     
	    /**
	     * This method generates random string
	     * @return
	     */
	    public String generateRandomString(){
	         
	        StringBuffer randStr = new StringBuffer();
	        for(int i=0; i<RANDOM_STRING_LENGTH; i++){
	            int number = getRandomNumber();
	            char ch = CHAR_LIST.charAt(number);
	            randStr.append(ch);
	        }
	        return randStr.toString();
	    }
	     
	    /**
	     * This method generates random numbers
	     * @return int
	     */
	    private int getRandomNumber() {
	        int randomInt = 0;
	        Random randomGenerator = new Random();
	        randomInt = randomGenerator.nextInt(CHAR_LIST.length());
	        if (randomInt - 1 == -1) {
	            return randomInt;
	        } else {
	            return randomInt - 1;
	        }
	    }
	    
	
}