package assignment2.NoCombiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
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
	
	//Default constructor
	TemperatureDataWritable() {
		this.temperature = new DoubleWritable();
		this.type = new Text();
	}
	
	//Initializes the member variables with the data from the parameters
	public TemperatureDataWritable(Text temperatureType, DoubleWritable temperatureValue) {
		this.temperature = temperatureValue;
		this.type = temperatureType;
	}
	
	//Serialization
	@Override
	public void write(DataOutput out) throws IOException { 
		temperature.write(out);
		type.write(out);
	}
	
	//returns the temperature
	public DoubleWritable getTemperature() {
		return this.temperature;
	}
	
	//returns the temperature type
	public Text getType() {
		return this.type;
	}
	
	//Deserialization
	@Override
	public void readFields(DataInput in) throws IOException { 
		temperature.readFields(in);
		type.readFields(in);
	}
	
	
}
