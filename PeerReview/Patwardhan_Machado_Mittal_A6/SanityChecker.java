import java.io.BufferedReader; 
import java.io.BufferedWriter; 
import java.io.DataInput; 
import java.io.DataOutput; 
import java.io.File; 
import java.io.FileInputStream; 
import java.io.FileNotFoundException; 
import java.io.IOException; 
import java.io.InputStreamReader; 
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList; 
import java.util.Date;
import java.util.HashMap; 
import java.util.HashSet; 
import java.util.Iterator; 
import java.util.List; 
import java.util.Map; 
import java.util.Random; 
import java.util.Set; 
import java.util.TreeMap; 
import java.util.Collections; 
import java.util.zip.GZIPInputStream; 
import java.io.FileWriter; 
import java.io.IOException; 
import java.io.BufferedWriter; 
 
/*
 * Authors: Neha Patwardhan, Joy Machado
 * */ 

public class SanityChecker
{
         // Sanity test to check if a record is valid or not
    	public static boolean sanityTest(String data[])
    	{
    		boolean saneData = true;
    		if(data.length != 110)
    			return false;

		if(data[40].equals("NA") || data[29].equals("NA") || data[50].equals("NA")
		   ||data[41].equals("NA") || data[30].equals("NA") || data[51].equals("NA")
		   || data[42].equals("NA") || data[43].equals("NA") || data[44].equals("NA"))
			return false;

		if(data[40].equals("") || data[29].equals("") || data[50].equals("")
		   ||data[41].equals("") || data[30].equals("") || data[51].equals("")
		   || data[42].equals("") || data[43].equals("") || data[44].equals(""))
			return false;
    	
    		
    		try{
    			int crsArrValue = convertToMinutes(data[40]);
	    		int crsDeptValue = convertToMinutes(data[29]);
	    		int crsElapsedValue = Integer.parseInt(data[50]);
	    		boolean crsTimes = crsTimeCheck(crsArrValue, crsDeptValue, crsElapsedValue);

	    		int originAirportId = Integer.parseInt(data[11]);
	    		int destionationAirportIDValue = Integer.parseInt(data[20]);
	    		int originAirportSeqIDValue = Integer.parseInt(data[12]);
	    		int destinationAirportSeqIDValue = Integer.parseInt(data[21]);
	    		int originCityMarketIDValue = Integer.parseInt(data[13]);
	    		int destinationCityMarketIDValue = Integer.parseInt(data[22]);
	    		int originStateFipsValue = Integer.parseInt(data[17]);
	    		int destinationStateFipsValue = Integer.parseInt(data[26]);
	    		int originWACValue = Integer.parseInt(data[19]);
	    		int destinationWACValue = Integer.parseInt(data[28]);
	    		boolean airPortChecks = airPortCheck(originAirportId, destionationAirportIDValue, originAirportSeqIDValue,
					destinationAirportSeqIDValue, originCityMarketIDValue,destinationCityMarketIDValue,originStateFipsValue,destinationStateFipsValue,originWACValue,destinationWACValue);
			String origin = data[14];
	    		String destination = data[23];
	    		String originCityName = data[15];
	    		String destinationCityName = data[24];
	    		String originState = data[18];
	    		String destState = data[27];
	    		String originStateName = data[16];
	    		String destStateName = data[25];
	    		boolean originDestinationChecks = orginDestinationCheck(origin, destination, originCityName, destinationCityName,
				     originState, destState, originStateName, destStateName);
	
			boolean cancelledFlight = true;
			String cancelled = data[47];
			boolean cancelledValue = Boolean.parseBoolean(cancelled); 
			if(cancelledValue)
			{
	    		int arrTimeValue = convertToMinutes(data[41]);
	    		int depTimeValue = convertToMinutes(data[30]);
	    		int actualElapsedTimeValue = Integer.parseInt(data[50]);
	    		
	    		int arrDelayValue = Integer.valueOf(data[42]);
	    		int arrDelayMinutes = Integer.valueOf(data[43]);

	    		boolean arr_del15Value = Boolean.parseBoolean(data[44]);
			

	    		cancelledFlight = cancelledFlights(cancelled, arrTimeValue,
				    depTimeValue, actualElapsedTimeValue, cancelledValue, arrDelayValue, arrDelayMinutes,
				    arr_del15Value, crsArrValue,  crsDeptValue, crsElapsedValue);		
			}
			saneData=(crsTimes&&airPortChecks&&originDestinationChecks&&cancelledFlight);
			return saneData;
		}
		catch(Exception e)
    		{
			return false;		
		}
    		
    	}
    	
    	// Check conditions when a flight has been cancelled
    	public static boolean cancelledFlights(String cancelled, int arrTimeValue,
    		    int depTimeValue, int actualElapsedTimeValue, boolean cancelledValue, int arrDelayValue,int arrDelayMinutes,
    		    boolean arr_del15Value,int CRSArrTimeValue, int  CRSDepTimeValue, int CRSElapsedTimeValue)
    		    {
    				if(cancelledValue)
    				{
    					int timeZone = CRSArrTimeValue - CRSDepTimeValue - CRSElapsedTimeValue;
    			        // ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
    					if((arrTimeValue -  depTimeValue - actualElapsedTimeValue - timeZone) != 0)
    						{
    							return false;
    						}
    						
    						// if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
    						if(arrDelayValue > 0)
    						{
    							if(arrDelayValue != arrDelayMinutes)
    								return false;
    						}
    						
    						// if ArrDelay < 0 then ArrDelayMinutes should be zero
    						if(arrDelayValue < 0)
    						{
    							if(arrDelayMinutes != 0)
    								return false;
    						}
    						
    						//if ArrDelayMinutes >= 15 then ArrDel15 should be true
    						if(arrDelayMinutes >= 15)
    						{
    							if(arr_del15Value != true)
    								return false;
    						}

    		    		}

    				return true;
    		}
    	
    	
    	
    	// Check for any blank value in origin and destination
    	public static boolean orginDestinationCheck(String origin, String destination,String originCityName, String destinationCityName,
    		    String originState, String destState, String originStateName, String destStateName)
    		    {
    				//Origin, Destination,  CityName, State, StateName should not be empty
    				if(origin.equals("") || destination.equals("") || originCityName.equals("") || destinationCityName.equals("")
    				  || originState.equals("") || destState.equals("") || originStateName.equals("") || destStateName.equals(""))
    					{
    						return false;
    					}
    				
    				return true;
    		    }
    	
    	// Check if the values associated with the origin and destination airport are in the valid format
    	public static boolean airPortCheck(int originAirportIDValue, int destionationAirportIDValue, int originAirportSeqIDValue,
    			int destinationAirportSeqIDValue, int originCityMarketIDValue, int destinationCityMarketIDValue,int originStateFipsValue,
    			int destinationStateFipsValue,int originWACValue,int destinationWACValue)
    		    {
    				//AirportID,  AirportSeqID, CityMarketID, StateFips, Wac for origin and destination should be larger than 0
    				if(originAirportIDValue <= 0 || destionationAirportIDValue <= 0 || originAirportSeqIDValue <= 0 || 
    				   destinationAirportSeqIDValue <= 0 || originCityMarketIDValue <= 0 || destinationCityMarketIDValue <= 0 ||
    				   originStateFipsValue <= 0 || destinationStateFipsValue <= 0 || originWACValue < 0 || destinationWACValue < 0)
    					{
    						return false;
    					}
    				return true;
    		    }
    	
    	
    	// Convert all time in the hh:mm to minutes
    	public static int convertToMinutes(String hoursMinutes)
    	{
    		int hours = 0;
    		int minutes = 0;

    		// The input is in the following format hhmm
    		if(hoursMinutes.length() == 4)
    		{
    			hours = Integer.parseInt(hoursMinutes.substring(0,2));
    			minutes = Integer.parseInt(hoursMinutes.substring(2));
    		}
    		else
    		{
    			// The input is present in the following format hmm
    			if(hoursMinutes.length() == 3)
    			{
    				hours = Integer.parseInt(hoursMinutes.substring(0,1));
    				minutes = Integer.parseInt(hoursMinutes.substring(1));
    			}
    			// The input could be present as mm or m
    			else
    			{
    				minutes = Integer.parseInt(hoursMinutes);
    			}
    		}
    		
    	   // Convert all the hours to minutes
    		minutes = hours*60 + minutes;
    		return minutes;
    	}
    	
    	// Check time zone conditions
    	public static boolean crsTimeCheck(int CRSArrTimeValue, int  CRSDepTimeValue, int CRSElapsedTimeValue)
    	{
    		if(CRSArrTimeValue == 0 || CRSDepTimeValue == 0)
    		{
    			return false;
    			
    		}
    		
    		int timeZone = CRSArrTimeValue - CRSDepTimeValue - CRSElapsedTimeValue;
    				
    		// If timeZone % 60 is not equal to 0 it does not pass the sanity check
    		if(timeZone % 60 != 0)
    		{
    			return false;
    		}
    				
    		return true;
    	}
    	

   
    
     
    // Main method   
     public static void main (String[] args) 
     {    
                  
     } 
     
}
             
             
    
