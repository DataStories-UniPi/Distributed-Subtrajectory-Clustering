package DSC;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import CustomWritables.DTJrPairElement;

public class DTJrPartitioner extends Partitioner<DTJrPairElement, Text> {

	@Override
	public int getPartition(DTJrPairElement key, Text value, int numReduceTasks) {

		String partition_key = Integer.toString(key.getr_obj_id()).concat(",").concat(Integer.toString(key.getr_traj_id()));
		
		return Math.abs(partition_key.hashCode() % numReduceTasks);
	}
}