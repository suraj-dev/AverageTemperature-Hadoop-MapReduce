package assignment2.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import assignment2.secondarysort.TemperatureDataWritable;

/*
 * TemperatureMapper class inherits from the Hadoop Mapper class
 * Input type of the class is Object, Text
 * It overrides the map method which emits output of type (StationYearPair, TemperatureDataWritable)
 */
public class TemperatureMapper extends Mapper<Object, Text, StationYearPair, TemperatureDataWritable>{
	
	/*
	 * Checks if the input record contains either TMAX or TMIN and emits the stationId as the key and
	 * temperatureType, temperature, count and year as the value.
	 */
	@Override
    protected void map(Object key, Text value,
        Context context) throws IOException, InterruptedException {
		String input = value.toString();
		String[] params = input.split(",");
		String stationId = params[0];
		String date = params[1];
		String temperatureType = params[2];
		String temperature = params[3];
		Double temperatureValue;
		try {
			temperatureValue = Double.parseDouble(temperature);
		} catch(Exception e) {
			temperatureValue = 0.0;
		}
		if(temperatureType.equals("TMAX") || temperatureType.equals("TMIN")) {
			int year;
			try {
			year = Integer.parseInt(date.substring(0, 4));
			} catch(Exception e) {
				year = 0;
			}
			context.write(new StationYearPair(new Text(stationId), new IntWritable(year)), 
					new TemperatureDataWritable(new Text(temperatureType), new DoubleWritable(temperatureValue), new IntWritable(1), new IntWritable(year)));
		}
	}
}
