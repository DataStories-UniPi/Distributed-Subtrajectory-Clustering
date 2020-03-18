package DSC;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import CustomWritables.DTJrPointElement;

public class PreprocessGroupingComparator extends WritableComparator {
	protected PreprocessGroupingComparator() {
		super(DTJrPointElement.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		DTJrPointElement key1 = (DTJrPointElement) w1;
		DTJrPointElement key2 = (DTJrPointElement) w2;
		

		int result = Integer.compare(key1.getobj_id(), key2.getobj_id());
		if (result == 0){
			result =Integer.compare(key1.gettraj_id(), key2.gettraj_id());
		}
		return result;


	}
}