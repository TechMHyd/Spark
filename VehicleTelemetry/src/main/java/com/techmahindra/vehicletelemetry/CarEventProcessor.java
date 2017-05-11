package com.techmahindra.vehicletelemetry;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;






import javassist.tools.Dump;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Function1;
import scala.Tuple2;
import scala.reflect.ClassTag;

import com.google.gson.Gson;
import com.techmahindra.vehicletelemetry.service.MaintenanceAnalyzerService;
import com.techmahindra.vehicletelemetry.service.UBIAnalyzerService;
import com.techmahindra.vehicletelemetry.utils.DumpCarEventsData;
import com.techmahindra.vehicletelemetry.utils.UBIUtils;
import com.techmahindra.vehicletelemetry.vo.CarEvent;
import com.techmahindra.vehicletelemetry.vo.UBIData;

public class CarEventProcessor implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger("CarEventProcessor");
	
	public CarEventProcessor(String topic, String persistStream, String persistProcessData) throws IOException, AnalysisException {
		// Get properties
		
		
		Properties props = new Properties();
		props.load(CarEventProcessor.class.getClassLoader().getResourceAsStream("vt.properties"));
		
		// Initialize
		int numThreads = Integer.parseInt(props.getProperty("KAFKA_THREAD_COUNT"));
		int batchDuration = Integer.parseInt(props.getProperty("KAFKA_BATCH_DURATION"));
		String zkQuorum = props.getProperty("ZK_QUORUM");
		String kafkaGroup = props.getProperty("KAFKA_GROUP");
		
		// Instantiate streaming context & spark session
		SparkConf sparkConf = new SparkConf().setAppName("CarEventsProcessor");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchDuration));
		jssc.checkpoint(".\\checkpoint");
		// Prepare topic Map
		Map<String, Integer> topicMap = new HashMap<>();
		topicMap.put(topic, numThreads);
		
		
		
		// Process streaming events
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, kafkaGroup, topicMap);
		JavaDStream<CarEvent> events = messages.map(new Function<Tuple2<String, String>, CarEvent>() {
			private static final long serialVersionUID = 1L;
			@Override
		    public CarEvent call(Tuple2<String, String> tuple2) {
				Gson gson = new Gson();
				return gson.fromJson(tuple2._2, CarEvent.class);
	    	}
	    });
		
		//jssc.sparkContext().parallelize(list);
		SparkSession spark = JavaSparkSessionSingleton.getInstance(sparkConf);
		Properties connProps = new Properties();
		connProps.put("user", props.getProperty("JDBC_USER"));
		connProps.put("password", props.getProperty("JDBC_PASSWORD"));
		connProps.put("driver", props.getProperty("JDBC_DRIVER"));
		final String fPersistStream=persistStream;
		final String fPersistProcessData=persistProcessData;
		
		System.out.println("Stream persistence enabled?:"+fPersistStream);
		System.out.println("UBI data persistence enabled?:"+fPersistProcessData);
		// Get events history and register as temp view
		try{
		Dataset<Row> dbDF = spark.read().jdbc(props.getProperty("JDBC_CONN_STRING"),props.getProperty("JDBC_TABLE"), connProps);
		dbDF.createOrReplaceTempView("events_history");
		}catch(Exception e)
		{
			System.out.println("<<*>>ERROR on locating database hence creating NEW DB!!!");
			try {
				DumpCarEventsData.initializeCarDB();
				Dataset<Row> dbDF = spark.read().jdbc(props.getProperty("JDBC_CONN_STRING"),props.getProperty("JDBC_TABLE"), connProps);
				dbDF.createOrReplaceTempView("events_history");
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		
		events.foreachRDD(new VoidFunction<JavaRDD<CarEvent>>() {
			int totalEvents=0;
			long startTime=0;
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaRDD<CarEvent> eventRDD) throws Exception {
				long eventCount=eventRDD.count();
				if( eventCount > 0) {

					SparkSession spark = JavaSparkSessionSingleton.getInstance(eventRDD.context().getConf());
					Dataset<Row> streamDF = spark.createDataFrame(eventRDD, CarEvent.class);
					
					if(streamDF != null /*&& streamDF.count() > 0*/) {
						streamDF.createOrReplaceTempView("events_stream");
						if("Y".equalsIgnoreCase(fPersistStream))
						{
								streamDF.write().mode(SaveMode.Append).jdbc(props.getProperty("JDBC_CONN_STRING"), props.getProperty("JDBC_TABLE"), connProps);
						}
					}
					
					//metrics processed for maintainance
					/*String sql = "SELECT history.vin as vin,history.city as city,history.model as model,avg(history.outsideTemp) as avg_outtemp,avg(history.engineTemp) as avg_enginetemp "
							+ "FROM events_history as history,events_stream as stream "
							+ "WHERE history.vin = stream.vin GROUP BY history.vin,history.city,history.model";
					Dataset<Row> avgTempData = spark.sql(sql);
					//avgTempData.show();
					MaintenanceAnalyzerService mas = new MaintenanceAnalyzerService();
					mas.process(avgTempData);*/
					
					//metrics processed for UBI
					String ubiSql="SELECT history.vin as vin"
							+ ",date_format(history.timestamp,'hh:mm:ss') as timestamp"
							+ ",date_format(history.timestamp,'YYYY-MM-dd') as date"
							//+ ",hour(history.timestamp) as hour"
							//+ ",date_format(history.timestamp,'E') as day"
							//+ ",history.timestamp as timestamp"
							+ ",(CASE WHEN hour(history.timestamp)>= 18 THEN 'night' ELSE 'day' END) AS tripTime"
			                + ",(CASE WHEN (date_format(history.timestamp,'E')='Sat' OR date_format(history.timestamp,'E')='Sun') THEN 'weekend' ELSE 'weekday' END) AS tripDay"
		                    +",history.tripId as tripId"
		                    +",history.incidentType as incidentType"
		                    +" FROM events_history as history,events_stream as stream"
		                    +" WHERE history.vin=stream.vin"
		                    +" GROUP BY history.timestamp,history.vin,history.incidentType,history.tripId"
		                    + " ORDER BY history.timestamp";

					Dataset<Row> ubiIncidentData = spark.sql(ubiSql);
					ubiIncidentData.createOrReplaceTempView("IncidentData");
					
					Dataset<Row> ubiTripsData = ubiIncidentData.groupBy("date","vin","tripId").count().orderBy(new Column("date"));
					ubiTripsData.createOrReplaceTempView("TripData");
					
					Dataset<Row> ubiAggregateData = spark.sql("select"
							+ " incident.date as date"
							+ ",incident.vin as vin"
							+ ",trip.count as trips"
							 + ",count(CASE WHEN incident.incidentType='OverSpeed' THEN incident.tripId ELSE NULL END) AS OSCount"
			                 + ",count(CASE WHEN incident.incidentType='HardBraking' THEN incident.tripId ELSE NULL END) AS HBCount"
			                 + ",count(CASE WHEN incident.incidentType='HardCornering' THEN incident.tripId ELSE NULL END) AS HCCount"
			                 + ",count(CASE WHEN incident.incidentType='HardAcceleration' THEN incident.tripId ELSE NULL END) AS HACount"
			                 + ",count(CASE WHEN incident.tripTime='night' THEN incident.tripId ELSE NULL END) AS NightTripCount"
			                 + ",count(CASE WHEN incident.tripDay='weekend' THEN incident.tripId ELSE NULL END) AS WekendTripCount"
							+ " FROM IncidentData as incident,TripData as trip"
							+ " WHERE incident.date = trip.date"
							+ " AND incident.vin = trip.vin"
							+ " GROUP BY incident.date,incident.vin,trip.count"
							+ " ORDER BY incident.date");
					
					
					//ubiAggregateData.write(). also try save aggregate and do process later
					
										
					Encoder<UBIData> ubiEncoder = Encoders.bean(UBIData.class);
					Dataset<UBIData> ubiAggregateData2 = ubiAggregateData.map(new MapFunction<Row, UBIData>() {
						private static final long serialVersionUID = 1L;
						@Override
						  public UBIData call(Row row) throws Exception {
							
							//System.out.println("testing it..");
							  UBIData data=new UBIData();
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

									float driverScorePerc=UBIAnalyzerService.getDriverScorePercentage(totalTrips, tripsHavingHB, tripsHavingHC, tripsHavingHA, tripsHavingOS);
									int driverScore=UBIAnalyzerService.getDriverScore(driverScorePerc);
									float discount=UBIAnalyzerService.getDiscount(totalTrips, tripsHavingHB, tripsHavingHC, tripsHavingHA, tripsHavingOS, noOfTripsNight, noOfTripsWeekend);
									//Run 1
									/*if("Y".equalsIgnoreCase(fPersistProcessData))
									{
										DumpCarEventsData.persistUBIData(vin,date,driverScore,driverScorePerc,discount);
									}*/
									data=new UBIData(vin,date,driverScore,driverScorePerc,discount);
								}
						    return data;//"Name: " + row.getString(0);
						  }
						}, ubiEncoder);
					//Run 1
					/*if("Y".equalsIgnoreCase(fPersistProcessData))
					{
						ubiAggregateData2.collect();
					}*/
					
					
					if("Y".equalsIgnoreCase(fPersistProcessData)) {
						if (ubiAggregateData2 != null )
						{
							//RUN 2
							//ubiAggregateData2.write().mode(SaveMode.Append).jdbc(props.getProperty("JDBC_CONN_STRING"), "UBI_DATA"/*props.getProperty("JDBC_TABLE")*/, connProps);
							//Run 3
							List<UBIData> ubiDataList=ubiAggregateData2.collectAsList();
							DumpCarEventsData.persistUBIAsBatch(ubiDataList);
						}
					}
					
					//UBIAnalyzerService ubiService = new UBIAnalyzerService();
					//ubiService.processIt(ubiAggregateData);
					//List<UBIData> ubiDataList=ubiService.process(ubiAggregateData);
					int rddEvents=Integer.valueOf(""+eventCount);
					totalEvents=totalEvents+rddEvents;
					
				}
				else	{
					if(totalEvents > 0){
						long millis=System.currentTimeMillis()-startTime;
						long minutes = (millis / 1000)  / 60;
						int seconds = Integer.valueOf(""+((millis / 1000) % 60));
						System.out.println("Start time:"+(new SimpleDateFormat("HH:mm:ss:SSS")).format(new Date(startTime)));
						System.out.println("End time:"+(new SimpleDateFormat("HH:mm:ss:SSS")).format(new Date(System.currentTimeMillis())));
						System.out.println("Total Events are :"+totalEvents+" processed time is:"+minutes+" minutes and "+seconds+"seconds!");
						System.out.println("No events to process!!!");
					}
					totalEvents=0;
					startTime=System.currentTimeMillis();
				}
			}
		});
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			jssc.stop();
			Thread.currentThread().interrupt();
			logger.error("Error::"+e.getMessage());
		}
	}
	
	public static void main(String[] args) throws IOException, AnalysisException {
		
		if (args.length < 1) {
			System.err.println("Usage: CarDataProcessor <topic> <options>");
	      System.exit(1);
	    }
		if(args.length < 2){
			System.out.println("No args passed considering default ones !!!");
			args=new String[]{args[0],"Y",UBIUtils.getPERSIST_DATA()};
		}else if(args.length < 3){
			args=new String[]{args[0],args[1],UBIUtils.getPERSIST_DATA()};
		}
		new CarEventProcessor(args[0],args[1],args[2]);
	}
}

/** Lazily instantiated singleton instance of SparkSession */
class JavaSparkSessionSingleton implements Serializable {
  private static final long serialVersionUID = 1L;
  private static transient SparkSession instance = null;
  private JavaSparkSessionSingleton(){}
  public static SparkSession getInstance(SparkConf sparkConf) {
    if (instance == null) {
      instance = SparkSession
        .builder()
        .config(sparkConf)
        .getOrCreate();
    }
    return instance;
  }
}