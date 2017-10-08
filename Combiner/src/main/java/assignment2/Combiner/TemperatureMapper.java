package assignment2.Combiner;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/*
 * TemperatureMapper class inherits from the Hadoop Mapper class
 * Input type of the class is Object, Text
 * It overrides the map method which emits output of type (Text, TemperatureDataWritable)
 */
public class TemperatureMapper extends Mapper<Object, Text, Text, TemperatureDataWritable>{
	
	/*
	 * Checks if the input record contains either TMAX or TMIN and emits the stationId as the key and
	 * temperatureType, temperature, count as the value.
	 */
	@Override
    protected void map(Object key, Text value,
        Context context) throws IOException, InterruptedException {
		String input = value.toString();
		String[] params = input.split(",");
		String stationId = params[0];
		String temperatureType = params[2];
		String temperature = params[3];
		Double temperatureValue = Double.parseDouble(temperature);
		if(temperatureType.equals("TMAX") || temperatureType.equals("TMIN"))
			context.write(new Text(stationId), 
					new TemperatureDataWritable(new Text(temperatureType), new DoubleWritable(temperatureValue), new IntWritable(1)));
	}
}
