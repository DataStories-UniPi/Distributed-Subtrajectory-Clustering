package DSC;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import CustomWritables.DTJrPairElement;

public class DTJrGroupingComparator extends WritableComparator {
	protected DTJrGroupingComparator() {
		super(DTJrPairElement.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		DTJrPairElement key1 = (DTJrPairElement) w1;
		DTJrPairElement key2 = (DTJrPairElement) w2;
		

		int result = Integer.compare(key1.getr_obj_id(), key2.getr_obj_id());
		if (result == 0){
			result =Integer.compare(key1.getr_traj_id(), key2.getr_traj_id());
		}
		return result;


	}
}