package com.uploadingfiles;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;


public class Helper {
	  //final static org.slf4j.Logger logger = LoggerFactory.getLogger(Helper.getClass());

	final static Logger logger = Logger.getLogger(Helper.class);
   	public static void listFilesForFolder(final File folder) throws Exception {
		//logger.info(folder.listFiles().length);
		
		
	    for (final File fileEntry : folder.listFiles()) {
	    	
	    	
	        if (!fileEntry.isDirectory()) {
	                               
	        	updateTier(fileEntry.getAbsoluteFile());
	        }
	    }
	    
	}
	
public static void updateTier(File inputFile) throws Exception {
        logger.info("Debug 1");
        PrintStream o = new PrintStream(new File("Console.txt"));
        PrintStream console = System.out;
        PreparedStatement pStmt      = null;
        PreparedStatement pStmt1     = null;
        logger.info("updateTier");
       
        ArrayList<Long> alnone = new ArrayList<Long>();
        ArrayList<Long> aldup = new ArrayList<Long>();
        ArrayList<Long> algood = new ArrayList<Long>();
       
        int count = 0;
       
     try {

        
		 BufferedReader br = new BufferedReader(new FileReader(inputFile));
            String[] mids = null;
               long mid = 0L;
               int tier = 0;
               String line = null;
              
               pStmt = getConnection().prepareStatement( SQL_UPDATE_TIER_LEVEL.toString() );
               pStmt1 = getConnection().prepareStatement( SQL_SELECT_DUP_MID.toString() );
               ResultSet rs = null;
               int ct = 0;

               while ((line=br.readLine()) != null){
                     count = count + 1;
                     mids = line.split(",");
                     mid = Long.parseLong(mids[0]);
                     tier = Integer.parseInt(mids[1]);
                     int preid = 1;
                    
                     pStmt1.setLong(preid, mid);
                     rs = pStmt1.executeQuery();
                     if (rs.next()) {
                            ct = rs.getInt(1);
                            if (ct==0){
                                   alnone.add(mid);
                            }
                            else if (ct >= 2){
                                   aldup.add(mid);
                            }
                            else {
                                   algood.add(mid);
                            }
                     }
                    
                     pStmt.setInt(preid++, tier);
                     pStmt.setLong(preid++, mid);
                     pStmt.executeUpdate();
               }                   
               br.close();
               pStmt.close();
               pStmt1.close();
               System.setOut(o);
               System.out.println("Total good = "+algood.size());
               System.out.println("Total none = "+alnone.size());
               System.out.println("none MID = "+alnone);
               System.out.println("Total Dup = "+aldup.size());
               System.out.println("none Dup = "+aldup);
               System.setOut(console);
               logger.debug("debug level log");
               logger.info("info level log");
               logger.error("error level log");
               logger.info("Total none = "+alnone.size());
            logger.info("none MID = "+alnone);
            logger.info("Total Dup = "+aldup.size());
            logger.info("none Dup = "+aldup);
     }
        catch (FileNotFoundException ex) {
               logger.info(ex.toString());
        }
        catch (Exception e){
               try{ getConnection().rollback(); } catch( Exception ee ) {
                     logger.info(ee.toString());
               }
               logger.info(e.toString());
        }
        finally
     {
               try    { 
                 if (pStmt != null ) 
                	 pStmt.close();
                
                 if (pStmt1 != null)
                	 pStmt1.close();
               } catch( Exception ignore ) {}
                    
              
               logger.info("Count="+count);
     }
 }
	private static final StringBuffer SQL_UPDATE_TIER_LEVEL = new StringBuffer("");
    private static final StringBuffer SQL_SELECT_DUP_MID = new StringBuffer("");
   
    static {           
           SQL_UPDATE_TIER_LEVEL.append(
                  "update merchant set tier_x_code = ? where merch_number = ?"
           );
    }
   
    static {           
           SQL_SELECT_DUP_MID.append(
                  "select count(1) from merchant where merch_number = ?"
           );
    }
	public static Connection getConnection() throws SQLException {
		Properties props = new Properties();
		FileInputStream fis = null;
		Connection con = null;
		try {
			fis = new FileInputStream("./db.properties");
			props.load(fis);
			Class.forName(props.getProperty("db.sac-prod-pool.driver"));
			con = DriverManager.getConnection(props.getProperty("db.sac-prod-pool.url"),
					props.getProperty("db.sac-prod-pool.user"),
					props.getProperty("db.sac-prod-pool.password"));
		} catch (IOException | ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			 //logger.info("DB connection closed");
			
		}
		return con;
	}

}


