import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBMSMonV4 {

	static final Logger logger = LoggerFactory.getLogger(DBMSMonV4.class);

	static Set sQuerySet = null;

	static HashMap<String, String> mSrcQueryMap = null;
	
	static String sServiceDB, sUrl, sDriver, sDbid, sDbpw, sSQLCheckFile, sResultDir;
	
	public static void main(String[] args) {

		logger.debug("DBMSMonV4 Job Start!!!");
		
		FileToInfo(args[0]); // Info File To Properties & DB Check SQL Load

        try {
        	
    		checkDB(sServiceDB, sUrl, sDriver, sDbid, sDbpw, sResultDir);
        	
        } catch (Exception e) {
        	logger.debug("Get Check DB Catch Exception : " + e.toString());
        } 
        
		logger.debug("DBMSMonV4 Job End!!!");
	}
	
	public static void FileToInfo(String sFile) {

		Properties prop = new Properties();
		InputStream is = null;

		try {
			is = new FileInputStream(sFile);
			prop.load(is);

			sServiceDB = prop.getProperty("SERVICE_DB");
			sUrl = prop.getProperty("JDBC_URL");
			sDriver = prop.getProperty("JDBC_DRIVER");
			sDbid = prop.getProperty("DB_ID");
			sDbpw = new String(Base64.decodeBase64(prop.getProperty("DB_PW").getBytes()));
			sSQLCheckFile = prop.getProperty("SQL_FILE");
			sResultDir = prop.getProperty("RESULT_FILE_DIR"); 
			
		} catch (Exception e) {
			logger.debug("FileToProps Load Info Catch Exception : " + e.toString());
		} finally {
			try {
				if (is != null)
					is.close();
			} catch (Exception e) {
				logger.debug("FileToProps Load Info Finally Exception : " + e.toString());
			}
		}

		Document document = null;
		Properties pJobSrcQuery = null;

		try {
			document = new SAXBuilder().build(new FileInputStream(sSQLCheckFile));
			pJobSrcQuery = new Properties();

			Element element = document.getRootElement();
			List childElementList = element.getChildren();

			for (int i = 0; i < childElementList.size(); i++) {
				pJobSrcQuery.setProperty(((Element) childElementList.get(i)).getName(),
						((Element) childElementList.get(i)).getValue());
			}

			mSrcQueryMap = new HashMap<String, String>((Map)pJobSrcQuery); //Set SQL
		} catch (Exception e) {
			logger.debug("FileToProps Load SQL Catch Exception : " + e.toString());
		}
	}
	
	public static void checkDB( String dbname, String dburl, String dbdriver, String dbid, String dbpw, String dir) {
		
		Connection dbConn = null;
		PreparedStatement dbPstmt = null;
		
		FileWriter fWrite = null;
				
		try {
			
			Class.forName(dbdriver);
			dbConn = DriverManager.getConnection(dburl, dbid, dbpw);
			
			sQuerySet = mSrcQueryMap.entrySet();
			
			ResultSet dbRslt = null;
			
			
			
			for (Object obj : sQuerySet) {
				
				Map.Entry queryEntry = (Map.Entry) obj;
				
				if(queryEntry.getKey().toString().trim().equals("SQL_CHECK")) {
					
					dbPstmt = dbConn.prepareStatement(queryEntry.getValue().toString().trim());
					dbRslt = dbPstmt.executeQuery();
					
					JSONArray jsonArr = new JSONArray();
					
					ResultSetMetaData rsmd = dbRslt.getMetaData();
					
					
					
					
					
					while (dbRslt.next()) {
						
						Date nowDate = new Date();
						SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmssSSS");
						
						fWrite = new FileWriter(dir + File.separator + simpleDateFormat.format(nowDate) + "_SQL_CHECK.json");
						
						int numColumns = rsmd.getColumnCount();
						
						JSONObject jsonObj = new JSONObject();
						  
						for (int i = 1; i <= numColumns; i++) {
							
							String column_name = rsmd.getColumnName(i);
						  
							jsonObj.put(column_name, dbRslt.getObject(column_name));
						}
						  
						//jsonArr.add(jsonObj);
						
						fWrite.write(jsonObj.toJSONString());
						fWrite.close();
					}
					
					
					
					
				}
				
				
				if(queryEntry.getKey().toString().trim().equals("FULL_CHECK")) {
					
					dbPstmt = dbConn.prepareStatement(queryEntry.getValue().toString().trim());
					dbRslt = dbPstmt.executeQuery();
					
					JSONArray jsonArr = new JSONArray();
					
					ResultSetMetaData rsmd = dbRslt.getMetaData();
					
					
					
					
					
					while (dbRslt.next()) {
						
						Date nowDate = new Date();
						SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmssSSS");
						
						fWrite = new FileWriter(dir + File.separator + simpleDateFormat.format(nowDate) + "_FULL_CHECK.json");
						
						int numColumns = rsmd.getColumnCount();
						
						JSONObject jsonObj = new JSONObject();
						  
						for (int i = 1; i <= numColumns; i++) {
							
							String column_name = rsmd.getColumnName(i);
						  
							jsonObj.put(column_name, dbRslt.getObject(column_name));
						}
						  
						//jsonArr.add(jsonObj);
						
						fWrite.write(jsonObj.toJSONString());
						fWrite.close();
					}
					
					
					
					
				}


				if(queryEntry.getKey().toString().trim().equals("LOCK_CHECK")) {
					
					dbPstmt = dbConn.prepareStatement(queryEntry.getValue().toString().trim());
					dbRslt = dbPstmt.executeQuery();
					
					JSONArray jsonArr = new JSONArray();
					
					ResultSetMetaData rsmd = dbRslt.getMetaData();
					
					
					
					
					
					while (dbRslt.next()) {
						
						Date nowDate = new Date();
						SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmssSSS");
						
						fWrite = new FileWriter(dir + File.separator + simpleDateFormat.format(nowDate) + "_LOCK_CHECK.json");
						
						int numColumns = rsmd.getColumnCount();
						
						JSONObject jsonObj = new JSONObject();
						  
						for (int i = 1; i <= numColumns; i++) {
							
							String column_name = rsmd.getColumnName(i);
						  
							jsonObj.put(column_name, dbRslt.getObject(column_name));
						}
						  
						//jsonArr.add(jsonObj);
						
						fWrite.write(jsonObj.toJSONString());
						fWrite.close();
					}
					
					
					
					
				}
			}
		} catch (Exception e) {
			logger.debug("DB Check Exception : " + e.toString());
		} finally {
			
			try {
				if(fWrite != null) {
					fWrite.close();
				}
				
				if(dbPstmt != null) {
					dbPstmt.close();
				}
				
				if(dbConn != null) {
					dbConn.close();
				}
			}catch (Exception e) {
				logger.debug("DB Check Exception : " + e.toString());
			}
		}
	}
}
