package assignment2.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * GroupingComparator class inherits the hadoop WritableComparable class
 * It provides methods that specify how two Writable Comparable objects should be compared in the reducer task
 */
public class GroupingComparator extends WritableComparator{
	
	protected GroupingComparator() {
		super(StationYearPair.class, true);
	}
	
	/*
	 * Specifies how StationYearPair objects should be compared inside each reduce task
	 * stations with the same stationId are grouped together
	 */
	public int compare(WritableComparable w1, WritableComparable w2) {
		StationYearPair sp1 = (StationYearPair) w1;
		StationYearPair sp2 = (StationYearPair) w2;

		int cmp = sp1.station.compareTo(sp2.station);
		return cmp;
	}
}
