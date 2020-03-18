package DSC;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import CustomWritables.DTJrPointElement;

public class PreprocessPartitioner extends Partitioner<DTJrPointElement, Text> {

	@Override
	public int getPartition(DTJrPointElement key, Text value, int numReduceTasks) {

		String partition_key = String.valueOf(key.obj_id).concat(",").concat(String.valueOf(key.traj_id));
		
		return Math.abs(partition_key.hashCode() % numReduceTasks);

	}
}