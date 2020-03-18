package DSC;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DatasetMBBDriver {
	
	public static String hostname = new String();
	public static String dfs_port = new String();
	public static String rm_port = new String();
	public static String workspace_dir = new String();
	public static String dtjmr_input_dir = new String();

    public static void main(String[] args) throws Exception {
    	
    	hostname = args[0];
     	dfs_port = args[1];
     	rm_port = args[2];
     	workspace_dir = args[3];
     	dtjmr_input_dir = args[4];

		Configuration conf = new Configuration();
    	
       	conf.set("fs.default.name", hostname.concat(dfs_port));
    	
    	conf.set("mapreduce.jobtracker.address", "local");
       	conf.set("mapreduce.framework.name", "yarn");
       	conf.set("yarn.resourcemanager.address", hostname.concat(rm_port));
       	
       	
    	FileSystem fs = FileSystem.get(conf);
    	if(fs.exists(new Path(workspace_dir.concat("/DatasetMBB")))){
    		fs.delete(new Path(workspace_dir.concat("/DatasetMBB")), true);
    	}

		Job job = Job.getInstance(conf, "DatasetMBB");
		
		job.setNumReduceTasks(1);
		
		job.setJarByClass(DatasetMBBDriver.class);

		job.setMapperClass(DatasetMBBMapper.class);
		
		job.setCombinerClass(DatasetMBBCombiner.class);

		job.setReducerClass(DatasetMBBReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);


		FileInputFormat.setInputPaths(job, new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(dtjmr_input_dir)));
		
		FileOutputFormat.setOutputPath(job, new Path(hostname.concat(dfs_port).concat(workspace_dir).concat("/DatasetMBB")));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

