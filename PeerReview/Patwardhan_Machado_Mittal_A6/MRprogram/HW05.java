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
 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.DoubleWritable; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.Writable; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 

/*
 * Authors: Neha Patwardhan, Joy Machado
 * */ 

public class HW05
{
	    
 // Custom object to store values for a carrier for map reduce analysis  
    static class FlightDetails implements Writable 
    { 
         
       Text carrier;
       Text date;
       Text origin;
       Text destination;
       Text crs_dept_time;
       Text dept_time;
       Text crs_arr_time;
       Text arr_time;
         
        FlightDetails()
        { 
            carrier = new Text();
            date = new Text();
            origin = new Text();
            destination = new Text();
            crs_dept_time = new Text();
            dept_time = new Text();
            crs_arr_time = new Text();
            arr_time = new Text();
        } 
        
        FlightDetails(FlightDetails flight)
        {
        	carrier = new Text(flight.carrier.toString());
            date = new Text(flight.date.toString());
            origin = new Text(flight.origin.toString());
            destination = new Text(flight.origin.toString());
            crs_dept_time = new Text(flight.crs_dept_time.toString());
            dept_time = new Text(flight.dept_time.toString());
            crs_arr_time = new Text(flight.crs_arr_time.toString());
            arr_time = new Text(flight.arr_time.toString());
        }
         
        FlightDetails(String c, String dt, String or, String dest, String crsDT, String dT,
        		String crsAT, String AT) 
        { 
            carrier = new Text(c);
            date = new Text(dt);
            origin = new Text(or);
            destination = new Text(dest);
            crs_dept_time = new Text(crsDT);
            dept_time = new Text(dT);
            crs_arr_time = new Text(crsAT);
            arr_time = new Text(AT);
        } 
         
        public void write(DataOutput dataoutput) throws IOException 
        { 
        	carrier.write(dataoutput); 
        	date.write(dataoutput); 
        	origin.write(dataoutput); 
        	destination.write(dataoutput); 
        	crs_dept_time.write(dataoutput); 
        	dept_time.write(dataoutput); 
        	crs_arr_time.write(dataoutput); 
        	arr_time.write(dataoutput); 
        } 
 
        @Override 
        public void readFields(DataInput in) throws IOException
        { 
        	carrier.readFields(in); 
        	date.readFields(in); 
        	origin.readFields(in); 
        	destination.readFields(in); 
        	crs_dept_time.readFields(in); 
        	dept_time.readFields(in); 
        	crs_arr_time.readFields(in); 
        	arr_time.readFields(in); 
        } 
   
    } 
     
   
    // Mapper Class
    public static class a2mapper extends Mapper<Object,Text,Text,FlightDetails> 
    { 
         // Sanity test to check if a record is valid or not
    	public static boolean sanityTest(String data[])
    	{
    		boolean saneData = true;
    		if(data.length != 110)
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
    	

    	// Map function
        public void map(Object key,Text value,Context context) throws IOException,InterruptedException             
        { 
            String line = value.toString(); 
            line = line.replaceAll("\"", ""); 
            String formatedData = line.replaceAll(", ", ":"); 
            String[] data = formatedData.split(","); 
            try
            {
            	// If a record passes the sanity test, process it ahead
	           if(sanityTest(data))
	            {
		               String origin = data[14];
		               String destination = data[23];
		               String crs_dept_time = data[29];
		               int crsDeptTime = Integer.parseInt(crs_dept_time);
		               String dept_time = data[30];
		               int DeptTime = Integer.parseInt(dept_time);
		               String crs_arr_time = data[40];
		               int crsArrTime = Integer.parseInt(crs_arr_time);
		               String arr_time = data[41];
		               int arrTime = Integer.parseInt(arr_time);
		               String carrier = data[8];
		               int year = Integer.parseInt(data[0]);
		               int month = Integer.parseInt(data[2]);
		               int day = Integer.parseInt(data[3]);
		               String date = year+"/"+month+"/"+day;
		               FlightDetails flight = new FlightDetails(carrier,date,origin,destination,crs_dept_time,dept_time,crs_arr_time,arr_time);
				
		               // The key for the mapper is the carrier and the year because
		               // we are supposed to calculate the number of missed connections
		               // for each airline per year.
		               context.write(new Text(carrier+"_"+year),flight);
		        } 
            }
    	catch(NumberFormatException e) 
    	{ 
       		
    	} 

         
    }
   } 
    
    // Reducer class
    public static class meanReducer extends Reducer<Text,FlightDetails,Text,Text> 
        { 
             // Reduce function
            public void reduce(Text key,Iterable<FlightDetails> flights,Context context) throws IOException,InterruptedException 
            { 
            	
            	ArrayList<FlightDetails> flightList = new ArrayList<FlightDetails>();
            	
            	Iterator<FlightDetails> iterator = flights.iterator();
                while(iterator.hasNext())
                {
                	FlightDetails flight = iterator.next();
                	FlightDetails flight_new = new FlightDetails(flight);
                	flightList.add(flight_new);
                }
                
            	
            	long connections = 0;
            	long missedConnections = 0;
            	for(FlightDetails flight : flightList)
            	{
            		// Get destination for a flight
            		String destination = flight.destination.toString();
            		
            		for(FlightDetails innerFlight : flightList)
            		{	
            			// Get origin for all the flights in the reducer for one carrier per year
            			String origin = innerFlight.origin.toString();
				
            			// If a destination  matches the origin of a files, we check for conditions
            			if(destination.equals(origin))
            			{
            				// Get time in the right format
            				String crsArrTime = convertToMinutes((flight.crs_arr_time).toString());
					
            				String crsDeptTime = convertToMinutes((innerFlight.dept_time).toString());
					
            				String arrDate = (flight.date).toString();
					
            				String deptDate = (innerFlight.date).toString();
					
            				String arrTime = convertToMinutes((flight.arr_time).toString());
					
            				String deptTime = convertToMinutes((innerFlight.dept_time).toString());
					
            				// Check if the diffrence in scheduled time for the flights is greater than
            				// 30 mins and less than (6*60) minutes(6 hours)
            				long crsDifference = timeDifference(arrDate+" "+crsArrTime, deptDate+" "+crsDeptTime);
            				if(crsDifference >= 30 && crsDifference <= 360)
            				{
            					connections++;
            					// Check conditions for the actual arrival time and departure time
            					// If they satisfy the condition, consider it a missed connection
            					long timeDifference = timeDifference(deptDate+" "+deptTime,arrDate+" "+arrTime);
						
            					if(timeDifference >= 30)
            						{
            							missedConnections++;
            						}
            				}
            				
					
            				
            			}	 //if
            		}	 //inner loop
	
            	} 	// outer loop
            	
            	if(connections !=0)
            	{
            		String carrierYear = key.toString();
            		String keys[] = carrierYear.split("_");
            		double precentage = ((double)missedConnections/(double)connections)*100;
            		context.write(new Text(keys[0]),new Text(keys[1]+"\t"+missedConnections+"\t"+precentage));
            	}
            } 
            
            
         // Convert all time in the hh:mm to minutes
    		public static String convertToMinutes(String hoursMinutes)
    		{
    			int hours = 0;
    			int minutes = 0;
    			String hr = "00";
    			String min = "00";
    		
    			// The input is in the following format hhmm
    			if(hoursMinutes.length() == 4)
		{
			hours = Integer.parseInt(hoursMinutes.substring(0,2));
			minutes = Integer.parseInt(hoursMinutes.substring(2));
			hr = hoursMinutes.substring(0,2);
			min = hoursMinutes.substring(2);
		}
		else
		{
			// The input is present in the following format hmm
			if(hoursMinutes.length() == 3)
			{
				hours = Integer.parseInt(hoursMinutes.substring(0,1));
				minutes = Integer.parseInt(hoursMinutes.substring(1));
				hr = "0"+hoursMinutes.substring(0,1);
				min = hoursMinutes.substring(1);
			}
			// The input could be present as mm or m
			else if(hoursMinutes.length() == 2)
			{
				minutes = Integer.parseInt(hoursMinutes);
				min = hoursMinutes;
			}
			else
			{
				minutes = Integer.parseInt(hoursMinutes);
				min = "0"+minutes;
			}
		}
			
    			return hr+":"+min;
    		}

    	// Find the difference in time for the dates and times passed to
    	// the function. The function return the delay in minutes
        public long timeDifference(String date1, String date2)
        {
        	SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm");
        	long timeDifference = 0;
        	try
        	{
        		Date arr_date = formatter.parse(date1);
        		Date dept_date = formatter.parse(date2);
        		long difference = dept_date.getTime() - arr_date.getTime();
        		timeDifference = difference/(1000*60);
        	}
        	catch (ParseException e)
        	{
        		e.printStackTrace();
        	}
        	
        	return timeDifference;
        }
        

            
        } 
     
    // Main method   
     public static void main (String[] args) throws InterruptedException,Exception 
     {    
                 Configuration conf = new Configuration(); 
                 Job job = Job.getInstance(conf, "word count"); 
                 job.setJarByClass(HW05.class); 
                  
                 job.setMapperClass(a2mapper.class); 
                           
                 job.setReducerClass(meanReducer.class); 
                 job.setMapOutputKeyClass(Text.class); 
                 job.setMapOutputValueClass(FlightDetails.class); 
                     
                 job.setOutputKeyClass(Text.class); 
                 job.setOutputValueClass(Text.class); 
 
                   
                FileInputFormat.addInputPath(job, new Path(args[0])); 
                FileOutputFormat.setOutputPath(job, new Path(args[1])); 
                if(job.waitForCompletion(true))
                { 
                        System.exit(0); 
                } 
      } 
     
}
             
             
    
