package assignment2.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/*
 * StationYearPair class inherits the hadoop WritableComparable class
 * It encapsulates the stationId and year data. Provides methods to set and retrieve them
 */
public class StationYearPair implements WritableComparable<StationYearPair>{
	public IntWritable year;
	public Text station;
	
	public StationYearPair() {
		this.year = new IntWritable();
		this.station = new Text();
	}
	
	//initializes the member variables
	public StationYearPair(Text station, IntWritable year) {
		this.year = year;
		this.station = station;
	}
	
	//Serialization
	@Override
	public void write(DataOutput out) throws IOException {
		// Let the default types implement their own write methods for their
		// types
		this.year.write(out);
		this.station.write(out);
	}

	//Deserialization
	@Override
	public void readFields(DataInput in) throws IOException {
		this.year.readFields(in);
		this.station.readFields(in);
	}

	/*
	 * specifies how to compare StationYearPair objects
	 */
	@Override
	public int compareTo(StationYearPair obj) {
		int cmp = this.station.compareTo(obj.station);

		if (cmp != 0) {
			return cmp;
		}

		return this.year.compareTo(obj.year);
	}
}
