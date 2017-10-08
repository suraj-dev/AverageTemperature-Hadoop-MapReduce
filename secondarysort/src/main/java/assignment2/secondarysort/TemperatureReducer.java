package assignment2.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import assignment2.secondarysort.TemperatureDataWritable;

/*
 * TemperatureReducer class inherits from the hadoop reducer class
 * Input to the class is StationYearPair, TemperatureDataWritable
 * overrides the reduce method which emits output of type Text,Text
 */
public class TemperatureReducer extends Reducer<StationYearPair, TemperatureDataWritable, Text, Text>{
	
	/*
	 * Iterates through the list of temperature data objects and calculates the average TMAX and TMIN values for each year.
	 * outputs the result as Text, Text
	 */
	@Override
	public void reduce(StationYearPair key, Iterable<TemperatureDataWritable> values, Context context)
			throws IOException, InterruptedException {
		Double tmax_running_sum = 0.0;
		int tmax_running_count = 0;
		Double tmin_running_sum = 0.0;
		int tmin_running_count = 0;
		Double tmax_average = 0.0;
		Double tmin_average = 0.0;
		int current_year = 0;
		int previous_year = 0;
		
		//used to build the output for each year
		StringBuilder outputString = new StringBuilder(); 
		outputString.append("[");
		
		for(TemperatureDataWritable temp : values) {
			current_year = temp.getYear().get();
			
			//Condition for the first iteration
			if(previous_year == 0) {
				previous_year = current_year;
			}
			
			if(current_year == previous_year) {
				if(temp.getType().toString().equals("TMAX")) {
					tmax_running_sum += temp.getTemperature().get();
					tmax_running_count++;
				}
				
				else if(temp.getType().toString().equals("TMIN")) {
					tmin_running_sum += temp.getTemperature().get();
					tmin_running_count++;
				}
			}
			
			else {
				if(tmax_running_count != 0) 
					tmax_average = tmax_running_sum/tmax_running_count;
				
				if(tmin_running_count != 0) 
					tmin_average = tmin_running_sum/tmin_running_count;
				
				outputString.append("(" + previous_year + ", " + tmin_average + ", " + tmax_average + "), ");
				tmax_running_sum = 0.0;
				tmax_running_count = 0;
				tmin_running_sum = 0.0;
				tmin_running_count = 0;
				tmax_average = 0.0;
				tmin_average = 0.0;
				if(temp.getType().toString().equals("TMAX")) {
					tmax_running_sum += temp.getTemperature().get();
					tmax_running_count++;
				}
				
				else if(temp.getType().toString().equals("TMIN")) {
					tmin_running_sum += temp.getTemperature().get();
					tmin_running_count++;
				}
			}
			
			previous_year = current_year;
			
		}
		
		//Calculation for last year for the station
		if(tmax_running_count != 0) 
			tmax_average = tmax_running_sum/tmax_running_count;
		
		if(tmin_running_count != 0) 
			tmin_average = tmin_running_sum/tmin_running_count;
		outputString.append("(" + current_year + ", " + tmin_average + ", " + tmax_average + ")]");
		context.write(key.station, new Text(outputString.toString()));
	}
}
