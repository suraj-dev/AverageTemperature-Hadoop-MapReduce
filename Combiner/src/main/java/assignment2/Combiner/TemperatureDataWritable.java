package assignment2.Combiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
/*
 * This class implements the Writable interface provided by Hadoop.
 * It encapsulates the temperature and the type of temperature(TMAX or TMIN)
 */
public class TemperatureDataWritable implements Writable{
	//holds the temperature value
	private DoubleWritable temperature;
	//holds the temperature type
	private Text type;
	private IntWritable count;
	
	//Default constructor
	TemperatureDataWritable() {
		this.temperature = new DoubleWritable();
		this.type = new Text();
		this.count = new IntWritable();
	}
	
	//Initializes the member variables with the data from the parameters
	public TemperatureDataWritable(Text temperatureType, DoubleWritable temperatureValue, IntWritable count) {
		this.temperature = temperatureValue;
		this.type = temperatureType;
		this.count = count;
	}
	
	//Serialization
	@Override
	public void write(DataOutput out) throws IOException { 
		temperature.write(out);
		type.write(out);
		count.write(out);
	}
	
	//returns the temperature
	public DoubleWritable getTemperature() {
		return this.temperature;
	}
	
	//returns the temperature type
	public Text getType() {
		return this.type;
	}
	
	//returns the count
	public IntWritable getCount() {
		return this.count;
	}
	
	//Deserialization
	@Override
	public void readFields(DataInput in) throws IOException { 
		temperature.readFields(in);
		type.readFields(in);
		count.readFields(in);
	}
	
	
}

