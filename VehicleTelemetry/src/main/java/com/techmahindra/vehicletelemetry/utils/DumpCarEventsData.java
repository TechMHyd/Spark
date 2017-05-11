package com.techmahindra.vehicletelemetry.utils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.techmahindra.vehicletelemetry.vo.UBIData;

public class DumpCarEventsData {
	
	private static Logger logger = LoggerFactory.getLogger("DumpCarEventsData");

	private static Properties props = new Properties();
	private static final String VT_PROP_FILE = "vt.properties";
	
	
	public void selectTheData() throws SQLException {

        Connection connection = getDBConnection();
        Statement stmt = null;
        try {
            connection.setAutoCommit(false);
            stmt = connection.createStatement();
            
            String sql1 = "SELECT history.vin as vin,history.city as city,history.model as model,avg(history.outsideTemp) as avg_outtemp,avg(history.engineTemp) as avg_enginetemp "
					+ "FROM events_history as history,events_stream as stream "
					+ "WHERE history.vin = stream.vin GROUP BY history.vin,history.city,history.model";
            
            ResultSet rs = stmt.executeQuery("SELECT vin"
            	/*	+ ",timestamp"
            		+ ",ROW_NUMBER() OVER () AS timestamp2"*/
            		+ ",FORMATDATETIME(timestamp,'YYYY-MM-dd') as timestamp"
            		//+ ",count(FORMATDATETIME(timestamp,'YYYY-MM-dd')) as count"
            //+ ",DATEADD(day, DATEDIFF(day, current_timestamp(), timestamp), current_timestamp()) as timestamp"
            //+ ",DATEADD(day, DATEDIFF(day, DATEADD(day,timestamp, timestamp), timestamp) as timestamp"
            +",count(tripid) as trips"
            + ",COUNT(CASE WHEN incidenttype='OverSpeed' THEN tripid ELSE NULL END) AS OSCount"
            + ",COUNT(CASE WHEN incidenttype='HardBraking' THEN tripid ELSE NULL END) AS HBCount"
            + ",COUNT(CASE WHEN incidenttype='HardCornering' THEN tripid ELSE NULL END) AS HCCount"
            + ",COUNT(CASE WHEN incidenttype='HardAcceleration' THEN tripid ELSE NULL END) AS HACount"
           // + ",COUNT(DATEADD(day, DATEDIFF(day, '2012-08-07T00:00:00.0000000Z', timestamp), '2012-08-07T00:00:00.0000000Z')) as count"
            +" FROM ("
            
            +" SELECT 4 as vin, '2008-04-01T14:48:15.3270345Z' as timestamp, '4_1' as tripid,'HardBraking' as incidenttype UNION ALL"
            +" SELECT 4, '2008-04-01T15:16:20.327Z', '4_1', 'HardBraking' UNION ALL"
            +" SELECT 4, '2008-04-01T15:16:22.327Z', '4_1', 'HardCornering' UNION ALL"
            +" SELECT 4, '2008-04-02T15:16:25.327Z', '4_1', 'HardAcceleration' UNION ALL"
            +" SELECT 4, '2008-04-02T15:16:25.327Z', '4_1', 'HardBraking' UNION ALL"
            +" SELECT 4, '2008-04-03T15:16:25.327Z', '4_1', 'HardBraking' UNION ALL"
            +" SELECT 4, '2008-04-03T13:23:30.767Z', '4_1', 'HardBraking' UNION ALL"
            +" SELECT 4, '2008-04-04T13:53:41.923Z', '4_1', 'OverSpeed' UNION ALL"
            
            +" SELECT 5, '2008-04-02T16:31:07.383Z', '5_1', 'OverSpeed' UNION ALL"
            +" SELECT 5, '2008-04-02T16:25:07.383Z', '5_1', 'HardBraking' UNION ALL"
            +" SELECT 5, '2008-04-02T16:28:07.383Z', '5_1', 'HardBraking' UNION ALL"
            +" SELECT 5, '2008-04-03T16:50:34.217Z', '5_1', 'HardCornering' UNION ALL"
            
            +" SELECT 3, '2008-04-04T13:41:41.627Z', '3_1', 'HardBraking' UNION ALL"
            +" SELECT 3, '2008-04-04T13:55:00.497Z', '3_1', 'HardCornering' ) A "
            
			+" WHERE vin=4 "
            +" GROUP BY "
            + "timestamp"
            //+ ",tripid"
           // + ", tripid"
            //+ ",timestamp" //DATEADD(day, DATEDIFF(day, '2012-08-07T00:00:00.0000000Z', timestamp), '2012-08-07T00:00:00.0000000Z')
            + " ORDER BY timestamp");
            //+ ",tripid");
            
            System.out.println("===================================");
            System.out.println("vin,timestamp,trips,oscount,hbcount,hccount,hacount");
            System.out.println("===================================");
            while (rs.next()) {
                System.out.println(/*rs.getString("vin") + 
                					" " + rs.getString("model")+*/
                					" " + rs.getString("vin")
                					//+" " + rs.getString("timestamp2")
                					+" " + rs.getString("timestamp")
                					+" " + rs.getString("trips")
                					//+" " + rs.getString("count")
                					+" " + rs.getString("OSCount")
                					+" " + rs.getString("HBCount")
                					+" " + rs.getString("HCCount")
                					+" " + rs.getString("HACount")
                					
                				   );
            }
            stmt.close();
            connection.commit();
        } catch (SQLException e) {
            System.out.println("Exception Message " + e.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    		
	}
	public void selectUBIData() throws SQLException {
        Connection connection = getDBConnection();
        Statement stmt = null;
        try {
            connection.setAutoCommit(false);
            stmt = connection.createStatement();
    		
            ResultSet rs = stmt.executeQuery("SELECT * from UBI_DATA");
            
            System.out.println("===================================");
            System.out.println("vin,date,driverScore,driverscorePerc,discount");
            System.out.println("===================================");
            while (rs.next()) {
                System.out.println(" " 
                					+ rs.getString("vin")
                					+" " + rs.getString("date")
                					+" " + rs.getString("driverScore")
                					+" " + rs.getString("driverscorePerc")
                					+" " + rs.getString("discount")
                				   );
            }
            stmt.close();
            connection.commit();
        } catch (SQLException e) {
            System.out.println("Exception Message " + e.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    		
	}
	
	public static String myName(String value) 
	{
    		return "Chidvilaas"+value;
    }
	
	public void selectCarEventData() throws SQLException {
        Connection connection = getDBConnection();
        Statement stmt = null;
        try {
            connection.setAutoCommit(false);
            stmt = connection.createStatement();
    		
            
            //stmt.execute("CREATE ALIAS MY_NAME FOR \"com.techmahindra.vehicletelemetry.utils.DumpCarEventsData.myName\" ");
            
            
            //ResultSet rs = stmt.executeQuery("SELECT MY_NAME('test') as vin from "
            ResultSet rs = stmt.executeQuery("SELECT count(*) as vin from "
            //ResultSet rs = stmt.executeQuery("SELECT * from "
           		+ "CAR");
          // + "UBI_DATA");//UBI_DATA
            		//+ " WHERE vin='Y3J9PV9TN36A4DUB9'");
            
            System.out.println("===================================");
            System.out.println("vin,date,driverScore,driverscorePerc,discount");
            System.out.println("===================================");
            while (rs.next()) {
                System.out.println(" " 
                					+ rs.getString("vin")
//                					+" " + rs.getString("date")
//                					+" " + rs.getString("driverScore")
//                					+" " + rs.getString("driverscorePerc")
                					
                				   );
            }
            stmt.close();
            connection.commit();
        } catch (SQLException e) {
            System.out.println("Exception Message " + e.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    		
	}
	
		
public void dumpEventsData() throws SQLException {
		Connection connection = getDBConnection();
		Statement stmt = null;
		try {
			connection.setAutoCommit(false);
			stmt = connection.createStatement();
			//stmt.execute("DROP TABLE CAR");
			//System.out.println("CAR TABLE DROPPED!!!");
			/*stmt.execute("CREATE TABLE IF NOT EXISTS CAR(vin varchar(20),"
					+ "model varchar(30),"
					+ "timestamp timestamp,"
					+ "outsidetemperature real,"
					+ "enginetemperature real,"
					+ "speed real,"
					+ "fuel real,"
					+ "engineoil real,"
					+ "tirepressure real,"
					+ "odometer bigint,"
					+ "city varchar(20),"
					+ "accelerator_pedal_position int,"
					+ "parking_brake_status int,"
					+ "headlamp_status int,"
					+ "brake_pedal_status int,"
					+ "transmission_gear_position varchar(10),"
					+ "ignition_status int,"
					+ "windshield_wiper_status int,"
					+ "abs int,"*/
			
			stmt.execute("CREATE TABLE IF NOT EXISTS "+props.getProperty("JDBC_TABLE")+"(vin varchar(20),"+
					"model varchar(30),"+
					"timestamp timestamp,"+
					"outsideTemp real,"+
					"engineTemp real,"+
					"speed real,"+
					"fuel real,"+
					"engineOil real,"+
					"tirePressure real,"+
					"odometer bigint,"+
					"city varchar(20),"+
					"accPedalPos int,"+
					"parkBrakeStatus int,"+
					"headlampStatus int,"+
					"brakePedalStatus int,"+
					"transGearPosition varchar(10),"+
					"ignitionStatus int,"+
					"windshieldWiperStatus int,"+
					"abs int,"
					+ "tripId varchar(28),"
					+ "incidentType varchar(20))");
			
			Properties props = new Properties();
			props.load(DumpCarEventsData.class.getClassLoader().getResourceAsStream("vt.properties"));
			String csvPath=props.getProperty("CSV_STREAM_PATH");
			
			String[] csvFiles = new String[]{
					/*"2080383215_33731b26e90f4669b71cfa2df85d3012_1.csv",
					"2080383215_63a0aef2ca984bb18c6dab552eae6b50_1.csv",
					"2080383215_6a2ed936c5614837a35ce1f8df1551e1_1.csv",
					"2080383215_7443ee0a2c914257934d34312be5dd38_1.csv",
					"2080383215_84900e51f4c84ada9018431d78ce4875_1.csv",
					"2080383215_9c80515754db4bb085754c4beb593d4f_1.csv",
					"2080383215_a2a770c27da642ee84a6306ef146eaf6_1.csv",
					"2080383215_c9a9f0d821ce4ba49596b7a26a2e5c2f_1.csv",
					"2080383215_f04a00196fa440f1b78686c8a3c01027_1.csv",
					"2080383215_f2f05d60bc4a4683ba7e1712af6127cc_1.csv",
					"2080383215_f4a20b74e7af490fbf0fc51bca68c38b_1.csv",
					"2080383215_fad06a5bcb1b4fbebec0c17711dc0824_1.csv",*/
					
					
						"824507016_30dc7f8019f24402a2df1a49c21689bf_2015-03-30.csv",
						"824507016_30dc7f8019f24402a2df1a49c21689bf_2015-04-28.csv",
						"824507016_77fddce51ace464f8dcd963cad9c796b_4.csv",
						"824507016_77fddce51ace464f8dcd963cad9c796b_2016-08-10.csv"
						//"824507016_77fddce51ace464f8dcd963cad9c796b_2.csv"*/
					/*"824507016_5244858dc7d84f0f9a47ffbf3692e7db_1.csv",*/
						//"824507016_77fddce51ace464f8dcd963cad9c796b_2.csv"/*,
					//"824507016_84390024f6b242a987805492bc92e521_1.csv"*/
			};

			for(String csvFile: csvFiles) {
				int count = stmt.executeUpdate("INSERT INTO CAR SELECT * FROM CSVREAD('" + csvPath + File.separator + csvFile + "', null, null)");
				logger.info("Inserted " + count + " records from " + csvFile);
			}
			
			//insertDynamicData(connection);
			/*String[] vins=getVins();
			for(String vin:vins)
			{
				//INSERT INTO UBI_DATA(vin,date, driverScore, driverScorePerc,discount) VALUES (?,?,?,?,?)
				int count = stmt.executeUpdate("INSERT INTO CAR (vin,"+
					"model,"+
					"timestamp,"+
					"outsideTemp,"+
					"engineTemp ,"+
					"speed,"+
					"fuel,"+
					"engineOil,"+
					"tirePressure,"+
					"odometer,"+
					"city,"+
					"accPedalPos,"+
					"parkBrakeStatus,"+
					"headlampStatus,"+
					"brakePedalStatus,"+
					"transGearPosition,"+
					"ignitionStatus,"+
					"windshieldWiperStatus,"+
					"abs,"
					+ "tripId,"
					+ "incidentType) VALUES("+vin+","
							+ ")");
				logger.info("Inserted " + count + " records from " + csvFile);
			}*/

			stmt.close();
			connection.commit();
		} catch (SQLException e) {
			logger.error("Exception Message:" + e.getLocalizedMessage());
			e.printStackTrace();
		} catch (Exception e) {
			logger.error("Exception:" + e.getLocalizedMessage());
			e.printStackTrace();
		} finally {
			connection.close();
		}
	}


private void insertDynamicData(Connection connection) {
	
	PreparedStatement pstmt=null;
	try {
		String query="INSERT INTO UBI_DATA(vin,date, driverScore, driverScorePerc,discount) VALUES (?,?,?,?,?) ";
		
		pstmt = connection.prepareStatement(query);
		/*pstmt.setString(1, vin);
		pstmt.setString(2, date);
		pstmt.setInt(3, driverScore);
		pstmt.setFloat(4,driverScorePerc);
		pstmt.setFloat(5,discount);
		*/
		int count=pstmt.executeUpdate();
		pstmt.close();
    	
		System.out.println("Inserted " + count + " records to UBI_DATA table! ");
		logger.info("Inserted " + count + " record(s) to UBI_DATA table! ");
		
	}catch(Exception e)
	{
		e.printStackTrace();
	}
	
}
private String[] getVins() {
	
	String[] vins=new String[]{
			"SECR0ECMYSH0YD1YA", "KFADKS5LP01N0Z7TW","KEB3UH45SYSONXL1Q","JSUOVVJFH8D183GCK","186QWIKBA7T5EYP2U",
			"0MKU5TFQWZO9HS11S", "9GSW2BOQQ3HKVJZZU","50EIKRNDAJN5UUJ4P","Q0GRV0ZZYK2TKGFW2","ORSUL9LZY92OIIR6L",
			"RC845HQH9LK2QALR1", "FUGSPCGFPW9L66ZWC","IA1N3EEX2BR2DD5HM","ZOPYIRVU7EI9GHUMQ","3HWY9QOTM78TUKUD8",
			"70B85HK639ITWWRV2", "75VT86IS1UV82VDDN","PGGCTO2VLSWPW8IIM","7X5AAFGEPWLZ10PQ6","KOXX7GFJ79EEJVF28",
			"QFD6C720QLI4JL5YJ", "6HJMCXBYWFJXUX7IM","VLPNYAXKVRK7HPOF8","C27GX6BM2GYV9HX9Y","E1U7K2VBNE5SZ0785",
			"QKZ2GA81V27UM0E5M", "4XBKFGGH188UOJQVV","TWSYNJXQ4PP3WXXM9","D9NR4FLZD8HYSZHEP","0EMM2KMRA875ZZRNG",
			"B4K1B6IHQG3NSA7B7", "AQTB6Y67HX3106CE2","JCK37G5T3HEBN0M33"
	};
return vins;
}



public static void persistUBIAsBatch(List<UBIData> ubiDataList) throws SQLException {
	Connection dbConnection = null;
	PreparedStatement pstmt = null;
	String query="INSERT INTO UBI_DATA(vin,date, driverScore, driverScorePerc,discount) VALUES (?,?,?,?,?) ";
	try {
		dbConnection = getDBConnection();
		
		dbConnection.setAutoCommit(false);
		Statement stmt = dbConnection.createStatement();
		stmt.execute("CREATE TABLE IF NOT EXISTS UBI_DATA(vin varchar(20),"+
				"date varchar(20),"+
				"driverScore int,"+
				"driverScorePerc real,"+
				"discount real)");
		stmt.close();
		
		pstmt = dbConnection.prepareStatement(query);
		dbConnection.setAutoCommit(false);
		for(UBIData ubiData:ubiDataList)
		{
			pstmt.setString(1, ubiData.getVin());
			pstmt.setString(2, ubiData.getDate());
			pstmt.setInt(3, ubiData.getDriverScore());
			pstmt.setFloat(4,ubiData.getDriverScorePerc());
			pstmt.setFloat(5,ubiData.getDiscount());
			pstmt.addBatch();
		}
		pstmt.executeBatch();
		dbConnection.commit();
		//System.out.println("Records inserted into UBI_DATA table!");
	} catch (SQLException e) {
		System.out.println(e.getMessage());
		dbConnection.rollback();
	} finally {
		if (pstmt != null) {
			pstmt.close();
		}
		if (dbConnection != null) {
			dbConnection.close();
		}
	}
}


	
	public static void persistUBIData(String vin, String date, int driverScore,	float driverScorePerc, float discount) throws SQLException {
		Connection connection = getDBConnection();
		Statement stmt = null;
		PreparedStatement pstmt=null;
		try {
			connection.setAutoCommit(false);
			stmt = connection.createStatement();
			stmt.execute("CREATE TABLE IF NOT EXISTS UBI_DATA(vin varchar(20),"+
					"date varchar(20),"+
					"driverScore int,"+
					"driverScorePerc real,"+
					"discount real)");
			stmt.close();
				// stmt.execute("INSERT INTO PERSON(id, name) VALUES(4, 'Anju')");
				// String InsertQuery = "INSERT INTO PERSON" + "(id, name) values" + "(?,?)";
				String query="INSERT INTO UBI_DATA(vin,date, driverScore, driverScorePerc,discount) VALUES (?,?,?,?,?) ";
				
				pstmt = connection.prepareStatement(query);
				pstmt.setString(1, vin);
				pstmt.setString(2, date);
				pstmt.setInt(3, driverScore);
				pstmt.setFloat(4,driverScorePerc);
				pstmt.setFloat(5,discount);
				
				int count=pstmt.executeUpdate();
				pstmt.close();
	        	
				//System.out.println("Inserted " + count + " records to UBI_DATA table! ");
				//logger.info("Inserted " + count + " record(s) to UBI_DATA table! ");
			
			connection.commit();
		} catch (SQLException e) {
			logger.error("Exception Message:" + e.getLocalizedMessage());
			e.printStackTrace();
		} catch (Exception e) {
			logger.error("Exception:" + e.getLocalizedMessage());
			e.printStackTrace();
		} finally {
			connection.close();
		}
	}
	
	
	public static void initializeCarDB() throws SQLException {
		Connection connection = getDBConnection();
		Statement stmt = null;
		PreparedStatement pstmt=null;
		try {
			connection.setAutoCommit(false);
			stmt = connection.createStatement();
			
			 //stmt.execute("CREATE ALIAS IF NOT EXISTS MyName FOR \"com.tm.h2.examples.MyFunctions.myName\" ");
			 
			stmt.execute("CREATE TABLE IF NOT EXISTS "+props.getProperty("JDBC_TABLE")+"(vin varchar(20),"+
					"model varchar(30),"+
					"timestamp timestamp,"+
					"outsideTemp real,"+
					"engineTemp real,"+
					"speed real,"+
					"fuel real,"+
					"engineOil real,"+
					"tirePressure real,"+
					"odometer bigint,"+
					"city varchar(20),"+
					"accPedalPos int,"+
					"parkBrakeStatus int,"+
					"headlampStatus int,"+
					"brakePedalStatus int,"+
					"transGearPosition varchar(10),"+
					"ignitionStatus int,"+
					"windshieldWiperStatus int,"+
					"abs int,"
					+ "tripId varchar(28),"
					+ "incidentType varchar(20))");
			
			stmt.close();
			System.out.println("Created new database with table '" + props.getProperty("JDBC_TABLE")+ "'!");
			//logger.info("Created new database with table '" + props.getProperty("JDBC_TABLE")+ "' !");
			connection.commit();
		} catch (SQLException e) {
			logger.error("Exception Message:" + e.getLocalizedMessage());
			e.printStackTrace();
		} catch (Exception e) {
			logger.error("Exception:" + e.getLocalizedMessage());
			e.printStackTrace();
		} finally {
			connection.close();
		}
	}
	
	public static Connection getDBConnection(){
		//Properties props = new Properties();
		Connection dbConnection=null;
        try {
        	props.load(DumpCarEventsData.class.getClassLoader().getResourceAsStream(VT_PROP_FILE));
            Class.forName(props.getProperty("JDBC_DRIVER"));
        } catch (ClassNotFoundException|IOException e) {
        	logger.error("CNFE", e);
			e.printStackTrace();
        }
        try {
            dbConnection = DriverManager.getConnection(props.getProperty("JDBC_CONN_STRING"), 
            		props.getProperty("JDBC_USER"), 
            		props.getProperty("JDBC_PASSWORD"));
        } catch (SQLException e) {
        	logger.error("SQLE", e);
			e.printStackTrace();
        }
        return dbConnection;
    }
	
	public static void main(String[] args) throws Exception {
        try {
         //new DumpCarEventsData().dumpEventsData();
         // new DumpCarEventsData().selectTheData();
       // new DumpCarEventsData().selectUBIData();
       new DumpCarEventsData().selectCarEventData();
           
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
	
}
