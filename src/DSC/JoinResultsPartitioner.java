package DSC;
//import java.util.StringTokenizer;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import CustomWritables.DTJSubtraj;

public class JoinResultsPartitioner extends Partitioner<DTJSubtraj, Text> {

	@Override
	public int getPartition(DTJSubtraj key, Text value, int numReduceTasks) {

		return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
}