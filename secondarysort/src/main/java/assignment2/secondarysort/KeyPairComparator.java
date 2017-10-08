package assignment2.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * KeyPair class inherits the hadoop WritableComparable class
 * It provides methods that specify how two Writable Comparable objects should be compared
 */
public class KeyPairComparator extends WritableComparator{
	
	protected KeyPairComparator() {
		super(StationYearPair.class, true);
	}

	/*
	 * Specifies how StationYearPair objects should be compared
	 */
	public int compare(WritableComparable w1, WritableComparable w2) {
		StationYearPair sp1 = (StationYearPair) w1;
		StationYearPair sp2 = (StationYearPair) w2;

		int cmp = sp1.station.compareTo(sp2.station);

		if (cmp != 0) {
			return cmp;
		}
		return sp1.year.compareTo(sp2.year);
	}
}
