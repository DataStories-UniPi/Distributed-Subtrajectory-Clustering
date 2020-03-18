package DSC;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueIntWritableInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;



public class EquiDepthHistDriver {
	
	public static String hostname = new String();
	public static String dfs_port = new String();
	public static String rm_port = new String();
	public static String workspace_dir = new String();
	public static String prepr_points_dir = new String();
	public static String dtjmr_input_dir = new String();
	public static double sample_freq;
	public static int max_nof_samples;

	
    public static void main(String[] args) throws Exception {
    	
    	hostname = args[0];
     	dfs_port = args[1];
     	rm_port = args[2];
     	workspace_dir = args[3];
     	prepr_points_dir = args[4];
     	dtjmr_input_dir = args[5];
     	sample_freq = Double.parseDouble(args[6]);
     	max_nof_samples = Integer.parseInt(args[7]);

    	Configuration conf = new Configuration();
    	
       	conf.set("fs.default.name", hostname.concat(dfs_port));
    	conf.set("mapreduce.jobtracker.address", "local");
       	conf.set("mapreduce.framework.name", "yarn");
       	conf.set("yarn.resourcemanager.address", hostname.concat(rm_port));

		FileSystem fs = FileSystem.get(conf);
    	if(fs.exists(new Path(workspace_dir.concat(dtjmr_input_dir)))){
    		fs.delete(new Path(workspace_dir.concat(dtjmr_input_dir)), true);
    	}

        Job job = Job.getInstance(conf, "Repartitioning");
       	

        job.setJarByClass(EquiDepthHistDriver.class);
        
        Path inputPath = new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(prepr_points_dir));
        Path partitionOutputPath = new Path(hostname.concat(dfs_port).concat(workspace_dir).concat("/TotalOrderPartitioner"));
        Path outputPath = new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(dtjmr_input_dir));
		
        YarnClient client = YarnClient.createYarnClient();
		client.init(new YarnConfiguration(conf));
		client.start();
	    List<NodeReport> reports = client.getNodeReports(NodeState.RUNNING);

	    int nof_nodes = reports.size();
		client.stop();
			    
        int nof_chunks = Math.max(nof_nodes, (int)Math.ceil(((double)fs.getContentSummary(inputPath).getSpaceConsumed()/(double)fs.getDefaultReplication(inputPath))/((double)fs.getDefaultBlockSize(inputPath))));

        job.setNumReduceTasks(nof_chunks);
        FileInputFormat.setInputPaths(job, inputPath);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionOutputPath);
        job.setInputFormatClass(KeyValueIntWritableInputFormat.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        
        // Write partition file with interval sampler
        InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(sample_freq, max_nof_samples);
        InputSampler.writePartitionFile(job, sampler);
        
        // add partition file to distributed cache
        job.addCacheFile(new URI(hostname.concat(dfs_port).concat(workspace_dir).concat("/TotalOrderPartitioner")));

        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(EquiDepthHistReducer.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    	        
    }
}
