package assignment2.Combiner;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
/*
 * TemperatureCombiner class inherits from the hadoop reducer class
 * It gets input of type Text, TemperatureDataWritable from the mappers
 * overrides the reduce method which emits output of type Text, TemperatureDataWritable
 */
public class TemperatureCombiner extends Reducer<Text, TemperatureDataWritable, Text, TemperatureDataWritable>{
	
	/*
	 * Iterates through the list of temperature data for each stationId key and computes the running TMAX and TMIN sums
	 * along with their respective count values
	 */
	@Override
    protected void reduce(Text key, Iterable<TemperatureDataWritable> values,
        Context context) throws IOException, InterruptedException {
		Double tmax_running_sum = 0.0;
		int tmax_running_count = 0;
		Double tmin_running_sum = 0.0;
		int tmin_running_count = 0;
		
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
		
		context.write(key, new TemperatureDataWritable(new Text("TMAX"), new DoubleWritable(tmax_running_sum), new IntWritable(tmax_running_count)));
		context.write(key, new TemperatureDataWritable(new Text("TMIN"), new DoubleWritable(tmin_running_sum), new IntWritable(tmin_running_count)));
	}
}
