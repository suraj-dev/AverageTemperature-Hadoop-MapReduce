package assignment2.InMapperCombiner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/*
 * TemperatureMapper class inherits from the Hadoop Mapper class
 * Input type of the class is Object, Text
 * It overrides the map method which updates the local accumulation data structure with temperature data for each
 * stationId in the map task
 */
public class TemperatureMapper extends Mapper<Object, Text, Text, TemperatureDataWritable>{
	
	//holds the stationId as the key and its corresponding running TMAX and TMIN values and also the counts as values
	public HashMap<String, TemperatureDataWritable> stations;
	
	//initializing the local accumulation data structure 
	@Override
	protected void setup(Context context) {
		this.stations = new HashMap<String, TemperatureDataWritable>();
	}
	
	/*
	 * checks for TMAX and TMIN values in the input records and updates the hashmap with 
	 * TMAX, TMIN values and respective counts for each stationId
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
		if(temperatureType.equals("TMAX") || temperatureType.equals("TMIN")) {
			String stationKey = stationId + "/" + temperatureType;
			if(!stations.containsKey(stationKey)) {
				stations.put(stationKey,
						new TemperatureDataWritable(new Text(temperatureType), new DoubleWritable(temperatureValue), new IntWritable(1)));
			}
			else {
				TemperatureDataWritable tdw = stations.get(stationKey);
				Double updatedTemperature = temperatureValue + tdw.getTemperature().get();
				int updatedCount = tdw.getCount().get() + 1;
				stations.put(stationKey, new TemperatureDataWritable(new Text(temperatureType), new DoubleWritable(updatedTemperature), new IntWritable(updatedCount)));
			}
		}
		
	}
	
	/*
	 * Iterates through the hashmap and emits each station and corresponding TMAX, TMIN and count values
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		Set<String> stationKeys = stations.keySet();
		String stationId;
		for (String key : stationKeys) {
			// Get only the stationId from the key
			stationId = key.split("/")[0];
			// Write to context with StationId and corresponding value in datastructure.
			context.write(new Text(stationId), stations.get(key));
		}
	}
}

