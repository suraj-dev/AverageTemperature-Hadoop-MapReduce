package assignment2.InMapperCombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/*
 * TemperatureReducer class inherits from the hadoop reducer class
 * Input to the class is Text, TemperatureDataWritable
 * overrides the reduce method which emits output of type Text,Text
 */
public class TemperatureReducer 
		extends Reducer<Text, TemperatureDataWritable, Text, Text>{
	
	/*
	 * Iterates through the list of temperature data objects and calculates the average TMAX and TMIN values.
	 * outputs the result as Text, Text
	 */
	@Override
    protected void reduce(Text key, Iterable<TemperatureDataWritable> values,
        Context context) throws IOException, InterruptedException {
		Double tmax_running_sum = 0.0;
		int tmax_running_count = 0;
		Double tmin_running_sum = 0.0;
		int tmin_running_count = 0;
		Double tmax_average = 0.0;
		Double tmin_average = 0.0;
		
		for(TemperatureDataWritable temp : values) {
			if(temp.getType().toString().equals("TMAX")) {
				tmax_running_sum += temp.getTemperature().get();
				tmax_running_count += temp.getCount().get();
			}
			
			else if(temp.getType().toString().equals("TMIN")) {
				tmin_running_sum += temp.getTemperature().get();
				tmin_running_count += temp.getCount().get();
			}
		}
		
		if(tmax_running_count != 0) 
			tmax_average = tmax_running_sum/tmax_running_count;
		
		if(tmin_running_count != 0) 
			tmin_average = tmin_running_sum/tmin_running_count;
		
		String result = tmin_average + ", " + tmax_average; 
		context.write(key, new Text(result));
	}
}

