package assignment2.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
/*
 * This class implements the Writable interface provided by Hadoop.
 * It encapsulates the temperature, the type of temperature(TMAX or TMIN), count and the year
 */
public class TemperatureDataWritable implements Writable{
	//holds the temperature value
	private DoubleWritable temperature;
	//holds the temperature type
	private Text type;
	private IntWritable count;
	private IntWritable year;
	
	//Default constructor
	TemperatureDataWritable() {
		this.temperature = new DoubleWritable();
		this.type = new Text();
		this.count = new IntWritable();
		this.year = new IntWritable();
	}
	//Initializes the member variables with the data from the parameters
	public TemperatureDataWritable(Text temperatureType, DoubleWritable temperatureValue, IntWritable count, IntWritable year) {
		this.temperature = temperatureValue;
		this.type = temperatureType;
		this.count = count;
		this.year = year;
	}
	
	//Serialization
	@Override
	public void write(DataOutput out) throws IOException { 
		temperature.write(out);
		type.write(out);
		count.write(out);
		year.write(out);
	}
	
	//returns the temperature
	public DoubleWritable getTemperature() {
		return this.temperature;
	}
	
	//returns the type of temperature
	public Text getType() {
		return this.type;
	}
	
	//returns the count
	public IntWritable getCount() {
		return this.count;
	}
	
	//returns the year
	public IntWritable getYear() {
		return this.year;
	}
	
	//Deserialization
	@Override
	public void readFields(DataInput in) throws IOException { 
		temperature.readFields(in);
		type.readFields(in);
		count.readFields(in);
		year.readFields(in);
	}	
}
