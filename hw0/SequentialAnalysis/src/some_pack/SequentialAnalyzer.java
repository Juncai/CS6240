package some_pack;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class SequentialAnalyzer {

	public static int corrupt_lines =0;
	public static int non_corrupt_lines = 0;
	public static HashMap<String, Float> meanPrices = new HashMap();
	public static HashMap<String, Integer> carrier_record_count = new HashMap();

	//Updates the value of a key by adding the avg ticket price to the already existing sum.
	public void updateTicketPrices(CSVRecord record)
	{
		String carrier = record.get("UNIQUE_CARRIER"); 
		float avg_tkt_price = Float.parseFloat(record.get("AVG_TICKET_PRICE"));
		if(carrier_record_count.containsKey(carrier))
		{
			//Updates the value if the key already exists in the Hash
			carrier_record_count.put(carrier, carrier_record_count.get(carrier)+1);
			meanPrices.put(carrier, avg_tkt_price+meanPrices.get(carrier));
		}
		else
		{
			//Adds the key value pair to the Hash if the key doesn't already exist in the Hash
			carrier_record_count.put(carrier, 1);
			meanPrices.put(carrier, avg_tkt_price);
		}
	}

	// Sorts the meanPrice hash map in increasing order 
	private static Map<String, Float> sortByComparator(Map<String, Float> unsortMap) {

		// Convert Map to List
		List<Map.Entry<String, Float>> list = 
				new LinkedList<Map.Entry<String, Float>>(unsortMap.entrySet());

		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<String, Float>>() {
			public int compare(Map.Entry<String, Float> o1,
					Map.Entry<String, Float> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});

		// Convert sorted map back to a Map
		Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
		for (Iterator<Map.Entry<String, Float>> it = list.iterator(); it.hasNext();) {
			Map.Entry<String, Float> entry = it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	//Displays the output in the expected format
	public void displayAirlinesMeanPrices()
	{
 
		//MeanPrice hash contains the sum of all the avg tickets of the corresponding airlines
		//Since now we have number of records for each carrier, update the value with the mean ticket price
		for(String carrier : meanPrices.keySet())
		{
			float meanprice = meanPrices.get(carrier)/carrier_record_count.get(carrier);
			meanPrices.put(carrier, meanprice);
		}

		//Sort the meanPrice hash by value in increasing order
		Map<String, Float> sortedMeanPrices = sortByComparator(meanPrices);
		for(String carrier : sortedMeanPrices.keySet())
		{
			
			BigDecimal bd = new BigDecimal(Float.toString(meanPrices.get(carrier)));
			bd = bd.setScale(2, BigDecimal.ROUND_HALF_UP);
			System.out.println(carrier + "\t"+ bd.floatValue());
		}

	}

	//Converts the hhmm (hours:mins) format to mins
	public int convert(int hhmm)
	{
		int total_mins;
		int mins = hhmm % 100;
		int hours = hhmm/100;
		total_mins = hours*60 + mins;
		return total_mins;
	}


	//Does sanity check for each record in the given data set
	public boolean sanityCheck(CSVRecord record)
	{

		try
		{
			int crs_arr_time = convert(Integer.parseInt(record.get("CRS_ARR_TIME")));
			if(crs_arr_time == 0)
			{
				System.out.println("crs arr time is zero. incremented corrupt lines");
				corrupt_lines++;
				return false;
			}

			int crs_dep_time = convert(Integer.parseInt(record.get("CRS_DEP_TIME")));
			if(crs_dep_time == 0)
			{
				System.out.println("crs dep time is zero. incremented corrupt lines");
				corrupt_lines++;
				return false;
			}

//						int crs_elapsed_time = Integer.parseInt(record.get("CRS_ELAPSED_TIME"));
//
//						if(crs_arr_time < crs_dep_time)
//							crs_arr_time = crs_arr_time + 1440;
//						
//						int timezone = crs_arr_time - crs_dep_time - crs_elapsed_time;
//						
//			
//			
//						
//						//System.out.println("crs_arr_time= "+crs_arr_time+"  crs_dep_time= "+crs_dep_time+"  crs_elapsed_time= "+crs_elapsed_time+"timezone= "+timezone+"  timezone%60 ="+(timezone % 60));
//						
//						if((timezone%60)!=0)
//						{
//							//System.out.println("timezone ="+timezone+ "timezone%60 ="+(timezone%60));
//							corrupt_lines++;
//							return false;
//						}

			int origin_airport_id= Integer.parseInt(record.get("ORIGIN_AIRPORT_ID"));
			int dest_airport_id= Integer.parseInt(record.get("DEST_AIRPORT_ID"));
			int origin_airport_seq_id = Integer.parseInt(record.get("ORIGIN_AIRPORT_SEQ_ID"));
			int dest_airport_seq_id = Integer.parseInt(record.get("DEST_AIRPORT_SEQ_ID"));
			int origin_city_market_id= Integer.parseInt(record.get("ORIGIN_CITY_MARKET_ID"));
			int dest_city_market_id= Integer.parseInt(record.get("DEST_CITY_MARKET_ID"));
			int origin_state_fips= Integer.parseInt(record.get("ORIGIN_STATE_FIPS"));
			int dest_state_fips= Integer.parseInt(record.get("DEST_STATE_FIPS"));
			int origin_wac= Integer.parseInt(record.get("ORIGIN_WAC"));
			int dest_wac= Integer.parseInt(record.get("DEST_WAC"));


			if(origin_airport_id <=0 || dest_airport_id <=0 || origin_airport_seq_id<=0 || dest_airport_seq_id<=0 || origin_city_market_id<=0 || dest_city_market_id<=0 || origin_state_fips <=0 || dest_state_fips<=0 || origin_wac<=0 || dest_wac<=0)
			{
				System.out.println("airport id or city market id or airport seq id or state fips or wac is not larger than zero");
				corrupt_lines++;
				return false;
			}

			String origin = record.get("ORIGIN");
			String destination= record.get("DEST");
			String origin_city_name = record.get("ORIGIN_CITY_NAME");
			String dest_city_name = record.get("DEST_CITY_NAME");
			String origin_state_abr = record.get("ORIGIN_STATE_ABR");
			String dest_state_abr = record.get("DEST_STATE_ABR");
			String origin_state_name= record.get("ORIGIN_STATE_NM");
			String dest_state_name= record.get("DEST_STATE_NM");

			if (origin == null 
					|| destination == null
					|| origin_city_name == null
					|| dest_city_name == null
					|| origin_state_abr == null
					|| dest_state_abr == null
					|| origin_state_name == null
					|| dest_state_name == null)
			{
				System.out.println("origin or dest or city name or state or state name is empty");
				corrupt_lines++;
				return false;
			}

			float avg_tkt_price = Float.parseFloat(record.get("AVG_TICKET_PRICE"));


						int cancelled = Integer.parseInt(record.get("CANCELLED"));
//						
//						int arr_time = convert(Integer.parseInt(record.get("ARR_TIME")));
//						int dep_time = convert(Integer.parseInt(record.get("DEP_TIME")));
//						int actual_elapsed_time = Integer.parseInt(record.get("ACTUAL_ELAPSED_TIME"));
//						
//						int arr_delay = Integer.parseInt(record.get("ARR_DELAY"));
//						int arr_delay_minutes = Integer.parseInt(record.get("ARR_DELAY_NEW"));
//						int arr_del15 = Integer.parseInt(record.get("ARR_DEL15"));
//					
//						
//						if(cancelled == 0)
//						{
//							
//							if(arr_time < dep_time)
//								arr_time = arr_time + 1440;
//							
//							int temp = (arr_time - dep_time - actual_elapsed_time - timezone);
//									
//							System.out.println("temp ="+temp);
//							
//							
//							if(temp != 0)
//							{
//								corrupt_lines++;		
//								return false;
//							} 
//							
//							
//							if(arr_delay>0)
//							{
//								
//								if(arr_delay != arr_delay_minutes)
//								{
//									corrupt_lines++;
//									
//									return false;
//								}
//								
//							}
//							
//							if(arr_delay<0)
//							{
//								if(arr_delay_minutes!=0)
//								{
//									corrupt_lines++;
//									return false;
//								}
//							
//							}
//							
//							
//							if(arr_delay_minutes >= 15)
//							{
//								
//								if(arr_del15==1)
//								{
//									corrupt_lines++;
//									return false;
//								}
//							}
//							
//						}



		}
		catch(NumberFormatException e)
		{
			//System.out.println("number format exception - : "+e.getMessage());
			corrupt_lines++;
			return false;
		}

		non_corrupt_lines++;
		return true;
	}

	//Main method
	public static void main(String[] args) throws IOException {

		SequentialAnalyzer sqa = new SequentialAnalyzer();

		try {

			CSVFormat csvFormat = CSVFormat.DEFAULT 
					.withIgnoreEmptyLines(true) 
					.withIgnoreSurroundingSpaces(true).withHeader(); 

			//Reads the input file
			FileInputStream stream = new FileInputStream(args[0]);
			InputStream gzipStream = new GZIPInputStream(stream);

			//Parses the CSV file within the given input .gz file
			CSVParser parser = csvFormat.parse(new InputStreamReader(gzipStream));

			//Iterates over each record and does sanity checks and keeps track of the mean prices for every unique carrier
			for(CSVRecord record : parser)
			{
				if(sqa.sanityCheck(record))
				{
					sqa.updateTicketPrices(record);	
				}
			}

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.print(corrupt_lines+"\t"+non_corrupt_lines+"\n");
		sqa.displayAirlinesMeanPrices();
	}
}




