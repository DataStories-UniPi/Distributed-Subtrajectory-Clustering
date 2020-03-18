package DSC;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import CustomWritables.DTJSubtraj;

public class JoinResultsDriver {
	
	public static String hostname = new String();
	public static String dfs_port = new String();
	public static String rm_port = new String();
	public static String workspace_dir = new String();
	public static String input_dir = new String();
	public static String subtraj_dir = new String();
	public static String output_dir = new String();
	public static int nof_reducers;
	public static boolean repr_only;

	
    public static void main(String[] args) throws Exception {
		
     	hostname = args[0];
     	dfs_port = args[1];
     	rm_port = args[2];
     	workspace_dir = args[3];
     	input_dir = args[4];
     	subtraj_dir = args[5];
     	output_dir = args[6];
     	nof_reducers = Integer.parseInt(args[7]);
     	repr_only = Boolean.parseBoolean(args[8]);


		Configuration conf = new Configuration();
		
       	conf.set("hostname", hostname);
       	conf.set("dfs_port", dfs_port);
       	conf.set("rm_port", rm_port);
       	conf.set("workspace_dir", workspace_dir);
       	conf.set("input_dir", input_dir);
       	conf.set("subtraj_dir", subtraj_dir);
       	conf.set("output_dir", output_dir);
       	conf.setInt("nof_reducers", nof_reducers);
       	conf.setBoolean("repr_only", repr_only);

       	conf.set("fs.default.name", hostname.concat(dfs_port));

		
    	conf.set("mapreduce.jobtracker.address", "local");
       	conf.set("mapreduce.framework.name", "yarn");
       	conf.set("yarn.resourcemanager.address", hostname.concat(rm_port));

       	conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.Lz4Codec");
   	
    	FileSystem fs = FileSystem.get(conf);
    	
    	if(fs.exists(new Path(workspace_dir.concat("/JoinResults")))){
    		fs.delete(new Path(workspace_dir.concat("/JoinResults")), true);
    	}
    	
		Job job = Job.getInstance(conf, "JoinResults");
		
		job.setNumReduceTasks(nof_reducers);
		
		job.setJarByClass(JoinResultsDriver.class);
		job.setPartitionerClass(JoinResultsPartitioner.class);
		job.setGroupingComparatorClass(JoinResultsGroupingComparator.class);
		job.setReducerClass(JoinResultsReducer.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(DTJSubtraj.class);
		job.setMapOutputValueClass(Text.class);

        Path subtrajPath = new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(subtraj_dir));
		FileStatus[] fileStatus = fs.listStatus(subtrajPath);
		for (FileStatus status : fileStatus) {
			job.addCacheFile(status.getPath().toUri());
		}

        MultipleInputs.addInputPath(job, new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(output_dir)), TextInputFormat.class, JoinResultsMapper2.class);
        MultipleInputs.addInputPath(job, new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(input_dir)), TextInputFormat.class, JoinResultsMapper1.class);

		
		FileOutputFormat.setOutputPath(job, new Path(hostname.concat(dfs_port).concat(workspace_dir).concat("/JoinResults")));

		job.waitForCompletion(true);

    }
}

