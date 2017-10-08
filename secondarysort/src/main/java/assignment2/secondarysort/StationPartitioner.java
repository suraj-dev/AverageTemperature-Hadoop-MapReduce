package assignment2.secondarysort;

import org.apache.hadoop.mapreduce.Partitioner;

/*
 * StationPartitioner class inherits from the hadoop Partitioner class
 * takes StationYearPair and TemperatureDataWritable as input
 * controls which StationYearPair objects go to which reducer
 */
public class StationPartitioner extends Partitioner<StationYearPair, TemperatureDataWritable>{
	
	/*
	 * Determines which reducer each StationYearPair object should be sent to
	 */
	@Override
	public int getPartition(StationYearPair key, TemperatureDataWritable value, int numPartitions) {
		return Math.abs(key.station.hashCode()) % numPartitions;
	}
}
