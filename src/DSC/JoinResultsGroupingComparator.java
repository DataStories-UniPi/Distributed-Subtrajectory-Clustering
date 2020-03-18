package DSC;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import CustomWritables.DTJSubtraj;

public class JoinResultsGroupingComparator extends WritableComparator {
	protected JoinResultsGroupingComparator() {
		super(DTJSubtraj.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		DTJSubtraj key1 = (DTJSubtraj) w1;
		DTJSubtraj key2 = (DTJSubtraj) w2;
		

		int result = key1.compareTo(key2);
		return result;


	}
}