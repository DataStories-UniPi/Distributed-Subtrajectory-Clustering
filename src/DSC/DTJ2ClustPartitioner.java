package DSC;

import org.apache.hadoop.mapreduce.Partitioner;

import CustomWritables.DTJSubtrajIntersectPart;
import CustomWritables.DTJSubtrajPairSim;

public class DTJ2ClustPartitioner extends Partitioner<DTJSubtrajIntersectPart, DTJSubtrajPairSim> {

	@Override
	public int getPartition(DTJSubtrajIntersectPart key, DTJSubtrajPairSim value, int numReduceTasks) {

		String partition_key = Integer.toString(key.obj_id).concat(",").concat(Integer.toString(key.traj_id).concat(",").concat(Integer.toString(key.subtraj_id)));
		
		return Math.abs(partition_key.hashCode() % numReduceTasks);
	}
}