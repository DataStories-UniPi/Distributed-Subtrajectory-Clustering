package DSC;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import CustomWritables.DTJSubtrajIntersectPart;

public class DTJ2ClustGroupingComparator extends WritableComparator {
	protected DTJ2ClustGroupingComparator() {
		super(DTJSubtrajIntersectPart.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		DTJSubtrajIntersectPart key1 = (DTJSubtrajIntersectPart) w1;
		DTJSubtrajIntersectPart key2 = (DTJSubtrajIntersectPart) w2;
		

		int result =Integer.compare(key1.obj_id, key2.obj_id);
		
		if (result == 0){
			result =Integer.compare(key1.traj_id, key2.traj_id);
		}
		if (result == 0){
			result =Integer.compare(key1.subtraj_id, key2.subtraj_id);
		}

		return result;


	}
}