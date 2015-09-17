/**
 * 
 */
package com.zuluwarrior.cdcsync;

import java.io.FileInputStream;
import java.sql.*;
import java.util.Date;

// old APIimport org.apache.log4j.Logger;
// import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * @author nedasi
 *
 */
public class Program {

	
	/**
	 * @param args
	 */
	
	public static org.apache.logging.log4j.Logger log4;  
//	public static java.sql.Connection sqlConn;
	public static String cdcsyncTable;
	public static String jdbcUrl;
	public static String u;
	public static String p;
	public static int insertFrequency;
	public static int selectFrequency;
	public static int threadRetryFrequency;
	public static int cleanerFrequency;
	public static int cleanupOlderThanHours;
	public static int cleanerMinRecordsToRemain;
	public static int cont = 1;
	public static String hostName;
	public static String ipAddress;
	public static String daemonU;
	public static String idText;
	
 
	public static void main(String[] args) {
		
	 // Add a shutdown hook to set cont to 0 so that threads have half a chance of shutting down in an orderly way
	 Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
            	Program.log4.info("Shutdown requested!");
            	Program.cont = 0;
                try {
                	Thread.sleep(1000);
                }
                catch (Exception e)
                {
                	System.exit(1234);
                }
                Program.log4.exit();
                System.exit(0);
            }
        });
	 	
		new com.zuluwarrior.cdcsync.Program();
		
		log4.info("..program ended");
	}	
	
	private static String getProperty(String key) {
		java.util.Properties properties = new java.util.Properties();
		String val = new String();
		try {
			properties.load(new FileInputStream("./conf/.properties"));
			val = properties.getProperty(key);
			log4.debug("Property "+key+" : "+val);
		} catch (Exception e) 
		{
		  log4.error(e.toString());
		}
		
				return val;
	}
	
	public Program() {
		try{
//			System.out.println("Program instantiated");
			System.setProperty("log4j.defaultInitOverride","true");
			System.setProperty("log4j.configurationFile","./conf/log4j.properties");
			System.setProperty("java.awt.headless", "true"); 
		}
		catch (Exception e)
		{
			System.out.println("Program crashed initilising log4j System Properties: "+e.toString());
			System.exit(7);
		}
		try
		{
			hostName = java.net.InetAddress.getLocalHost().getHostName();
			daemonU = System.getProperty("user.name");
			ipAddress = java.net.InetAddress.getLocalHost().toString();
			idText = daemonU+"@"+hostName+" ("+ipAddress+")";
			log4 = LogManager.getLogger(com.zuluwarrior.cdcsync.Program.class.getName());
			jdbcUrl = new String(getProperty("jdbcUrl"));
			u = new String(getProperty("tableU"));
			p = new String(getProperty("tableP"));
			insertFrequency = Integer.parseInt(getProperty("insertFrequency"))*1000;
			selectFrequency = Integer.parseInt(getProperty("selectFrequency"))*1000;
			threadRetryFrequency = Integer.parseInt(getProperty("threadRetryFrequency"))*1000;
			cleanerFrequency = Integer.parseInt(getProperty("cleanerFrequency"))*1000;
			cleanupOlderThanHours = Integer.parseInt(getProperty("cleanupOlderThanHours"));
			cleanerMinRecordsToRemain = Integer.parseInt(getProperty("cleanerMinRecordsToRemain"));
			cdcsyncTable = new String(getProperty("cdcsyncTable"));
			
			// TODO Auto-generated method stub
		}
		catch (Exception e)
		{
			log4.error("Error initialising properties "+e.toString());
			System.exit(5);
		}
		log4.info("CDC Table Sync program starting");
		Thread tI = new InsertSqlThread();
		Thread tS = new SelectSqlThread();
		Thread tC = new TableCleanerThread();

		while (cont == 1)
		{			
			if ( tI.getState().equals(Thread.State.NEW) || tI.getState().equals(Thread.State.TERMINATED))   
				{
					Program.log4.debug("Thread State: "+tI.getState().toString()+". Starting SQL Insert Thread Loop");
					tI.start();
				}
			if ( tS.getState().equals(Thread.State.NEW) || tS.getState().equals(Thread.State.TERMINATED))
			{
				Program.log4.debug("Thread State: "+tS.getState().toString()+". Starting SQL Select Thread Loop");
				tS.start();
			}
			if ( tC.getState().equals(Thread.State.NEW) || tC.getState().equals(Thread.State.TERMINATED))
			{
				Program.log4.debug("Thread State: "+tC.getState().toString()+". Starting SQL Cleaner Thread Loop");
				tC.start();
			}
			try {
				Thread.sleep(threadRetryFrequency);
			}
			catch (Exception e)
			{
				Program.log4.error("Thread restart delay exception");
				System.exit(1200);
			}
		}

	}
}

class InsertSqlThread extends Thread
{
   public void run ()
   {
	   Program.log4.info("SQL insert worker thread starting");
	   insertWorker();
   }

   private synchronized static void insertWorker() {       	
    	while (Program.cont == 1)
    	{
    		java.sql.Connection sqlConn;   		
    		
    	   	try {
    			Program.log4.debug("Creating SQL insert connection");
    			java.sql.DriverManager.registerDriver(new com.ibm.as400.access.AS400JDBCDriver());
    			sqlConn = java.sql.DriverManager.getConnection(Program.jdbcUrl, Program.u, Program.p);
				Program.log4.debug("Performing SQL insert");
				Statement stmt = sqlConn.createStatement();		
				java.sql.Timestamp ts2 = new java.sql.Timestamp(new Date().getTime());
				Program.log4.info("SQL insert values '"+Program.idText+"' '"+ts2+"'");
				stmt.executeUpdate("insert into "+Program.cdcsyncTable+" (FREEFORM,TS2) values ('"+Program.idText+"', '"+ts2+"')");
				sqlConn.close();
    		}
    		catch (Exception e) 
    		{
    			if (e instanceof java.awt.HeadlessException) {
    				Program.log4.error("Insert Worker Thread: Login to OS400 failed: URL:"+Program.jdbcUrl+", User:"+Program.u+" Password:****. Retyring again later");
    				Program.log4.debug(e.toString());
    			}
    			else
    			{
    				Program.log4.error(e.toString());
    			}
    		}
    		try {
    			Thread.sleep(Program.insertFrequency);
    		}
    		catch (Exception e3)
    		{
    			Program.log4.error(e3.toString());
    			return;
   			}    		
    	}
	}
}

class SelectSqlThread extends Thread
{
	   public void run ()
	   {
		   Program.log4.info("SQL select worker thread starting");
		   selectWorker();
	   }


	   private synchronized static void selectWorker() {       	
	    	while (Program.cont == 1)
	    	{
	    	   	java.sql.Connection sqlConn;
	    		try {
	    			Program.log4.debug("Creating SQL select connection");
	    			java.sql.DriverManager.registerDriver(new com.ibm.as400.access.AS400JDBCDriver());
	    			sqlConn = java.sql.DriverManager.getConnection(Program.jdbcUrl, Program.u, Program.p);
    				Program.log4.debug("Performing SQL select");
    				Statement stmt = sqlConn.createStatement();
    			    ResultSet rs = stmt.executeQuery("SELECT PKEY,TS1, TS2, FREEFORM from "+Program.cdcsyncTable+" order by TS2 desc fetch first 1 rows only");
    			    while (rs.next()) {
    			    	Program.log4.info("Last row selected "+new Integer(rs.getInt("PKEY")).toString()+":OS400 TS1:"+rs.getTimestamp("TS1").toString()+"\tUNIX TS2:"+rs.getTimestamp("TS2").toString()+"\t\""+rs.getNString("FREEFORM")+"\"");
    			    }
    				sqlConn.close();
	    		}
	    		catch (Exception e)
	    		{
	    			if (e instanceof java.awt.HeadlessException) {
	    				Program.log4.error("Select Worker Thread: Login to OS400 failed: URL:"+Program.jdbcUrl+", User:"+Program.u+" Password:****. Retyring again later");
	    				Program.log4.debug(e.toString());
	    			}
	    			else
	    			{
	    				Program.log4.error(e.toString());
	    			}
	    		}    		
   			try {
   				Thread.sleep(Program.selectFrequency);
   			}
    		catch (Exception e3)
    		{
    			Program.log4.error(e3.toString());
    			return;
   			}    		
    	}
	}
}

class TableCleanerThread extends Thread
{
   public void run ()
   {
	   Program.log4.info("Table Cleaner thread starting");
	   cleanerWorker();
   }

   private synchronized static void cleanerWorker() { 
   	while (Program.cont == 1)
   	{
		try {
			Thread.sleep(Program.cleanerFrequency);
		}
		catch (Exception e)
		{
			Program.log4.error(e.toString());
			return;							
		}    		

   	   	java.sql.Connection sqlConn;  	
   		try {
   			Program.log4.debug("Creating SQL cleaner connection");
   			java.sql.DriverManager.registerDriver(new com.ibm.as400.access.AS400JDBCDriver());
   			sqlConn = java.sql.DriverManager.getConnection(Program.jdbcUrl, Program.u, Program.p);
			Program.log4.info("Performing table cleanup");   				
			java.util.Calendar cal = java.util.Calendar.getInstance();
			//Program.log4.debug("Calendar now "+cal.toString());
			cal.add(java.util.Calendar.HOUR_OF_DAY, (Program.cleanupOlderThanHours/-1));
			//Program.log4.debug("Calendar minus "+Program.cleanupOlderThanHours+" hours "+cal.toString());
			java.sql.Timestamp ts2 = new java.sql.Timestamp(cal.getTimeInMillis());	
			Statement stmt = sqlConn.createStatement();
			//Program.log4.debug("SELECT PKEY,TS1, TS2, FREEFORM from operations.cdcsync where TS2 < '"+ts2+"'");
			Program.log4.debug("delete from "+Program.cdcsyncTable+" where TS2 < '"+ts2+"' and PKEY not in (select PKEY from operations.cdcsync order by TS2 asc fetch first "+Program.cleanerMinRecordsToRemain+" rows only)"); 
			int rs = stmt.executeUpdate("delete from "+Program.cdcsyncTable+" where TS2 < '"+ts2+"' and PKEY not in (select PKEY from operations.cdcsync order by TS2 asc fetch first "+Program.cleanerMinRecordsToRemain+" rows only)");
			Program.log4.info("Number of records cleaned up : "+new Integer(rs).toString());
			sqlConn.close();
   		} 		
   		catch (Exception e)
   		{
   			if (e instanceof java.awt.HeadlessException) {
   				Program.log4.error("Cleaner Worker Thread: Login to OS400 failed: URL:"+Program.jdbcUrl+", User:"+Program.u+" Password:****. Retyring again later");
   				Program.log4.debug(e.toString());
   			}
 			else
 			{
   				Program.log4.error(e.toString());
 			}
		}		
	}
}
   
}
