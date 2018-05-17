package com.uploadingfiles;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;


public class Util {

	
	
	
	
	
	public static void listFilesForFolder(final File folder) throws IOException, SQLException, URISyntaxException {
		System.out.println(folder.listFiles().length);
		
		
	    for (final File fileEntry : folder.listFiles()) {
	    	
	    	
	        if (!fileEntry.isDirectory()) {
	                               
	        	readCSV(fileEntry.getAbsoluteFile());
	        }
	    }
	    
	}
	
	public static Connection getConnection() throws SQLException {
		Properties props = new Properties();
		FileInputStream fis = null;
		Connection con = null;
		try {
			fis = new FileInputStream("./db.properties");
			props.load(fis);
			Class.forName(props.getProperty("db.atx-test-pool.driver"));
			con = DriverManager.getConnection(props.getProperty("db.atx-test-pool.url"),
					props.getProperty("db.atx-test-pool.user"),
					props.getProperty("db.atx-test-pool.password"));
				  
			 
		} catch (IOException | ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			 //System.out.println("DB connection closed");
			
		}
		return con;
	}
	
	
	
	 
	 public static void readCSV(File inputFile) throws IOException, SQLException, URISyntaxException
	 {
		 BufferedReader reader = new BufferedReader(new FileReader(inputFile));
String allGood	;
			String cvsSplitBy = ",";
			while((allGood = reader.readLine()) != null) {

				
			    String trimmedLine = allGood.trim();
			    String[] result= trimmedLine.trim().split(cvsSplitBy);
			   
			    insertdatabse(result[0],result[1]);
			    
			    
			}

	 }
	 
	 
	 public static void insertdatabse(String merch_number,String tier_x_code) throws SQLException, URISyntaxException
{

		 try{
			   
			 String updateTableSQL = "update  mes.merchant set  tier_x_code=? where merch_number =? ";
				PreparedStatement preparedStatement = getConnection().prepareStatement(updateTableSQL);
				//preparedStatement.setInt(1,Integer.parseInt(tier_x_code));
				preparedStatement.setLong(1, Long.parseLong(tier_x_code));
				preparedStatement.setString(2,merch_number);
			     int rowsUpdated = preparedStatement .executeUpdate();
			    System.out.println("Updated Successfully!!  "+"\t"+"tier_x_code "+"\t"+ tier_x_code +" merch_number "+"\t"+merch_number);
			   
			    preparedStatement.close();
			    //System.out.println("Updated");
				
		 }
		
		 finally{
			 
			getConnection().close();
		 }
	
 	    
	
		 
		 
		 
		 }


	 
	 }
