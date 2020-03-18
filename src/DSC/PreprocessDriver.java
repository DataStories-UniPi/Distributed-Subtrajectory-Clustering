package DSC;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.StringTokenizer;

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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.CreateOctTreePartitionFile;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import CustomWritables.DTJrPointElement;


public class PreprocessDriver {
	
	public static String hostname = new String();
	public static String dfs_port = new String();
	public static String rm_port = new String();
	public static String workspace_dir = new String();
	public static String raw_dir = new String();
	public static String prep_dir = new String();
	public static String input_dir = new String();
	public static double sample_freq;
	public static int max_nof_samples;
	public static String[] dMBB;
	public static Double maxCellPtsPrcnt = 0.0;

    public static void main(String[] args) throws Exception {
     	
    	hostname = args[0];
     	dfs_port = args[1];
     	rm_port = args[2];
     	workspace_dir = args[3];
     	raw_dir = args[4];
     	prep_dir = args[5];
     	input_dir = args[6];
     	sample_freq = Double.parseDouble(args[7]);
     	max_nof_samples = Integer.parseInt(args[8]);
     	maxCellPtsPrcnt = Double.parseDouble(args[9]);

    	Configuration conf = new Configuration();
    	FileSystem fs = FileSystem.get(conf);

       	conf.set("hostname", hostname);
       	conf.set("dfs_port", dfs_port);
       	conf.set("rm_port", rm_port);
       	conf.set("workspace_dir", workspace_dir);
       	conf.set("raw_dir", raw_dir);
       	conf.set("prep_dir", prep_dir);
       	conf.set("input_dir", input_dir);
       	conf.setDouble("maxCellPtsPrcnt", maxCellPtsPrcnt);

       	conf.set("fs.default.name", hostname.concat(dfs_port));
       	
    	conf.set("mapreduce.jobtracker.address", "local");
       	conf.set("mapreduce.framework.name", "yarn");
       	conf.set("yarn.resourcemanager.address", hostname.concat(rm_port));
       	
    	
        Path job1_inputPath = new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(raw_dir));
        Path job1_outputPath = new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(prep_dir));

    	if(fs.exists(job1_outputPath)){
    		fs.delete(job1_outputPath, true);
    	}
    	
		Job job1 = Job.getInstance(conf, "preprocess");


		
		job1.setJarByClass(PreprocessDriver.class);
		
		//Get number of Nodes
		YarnClient client = YarnClient.createYarnClient();
		client.init(new YarnConfiguration(conf));
		client.start();
	    List<NodeReport> reports = client.getNodeReports(NodeState.RUNNING);
	    int nof_nodes = reports.size();
		client.stop();

		int job1_nof_chunks = Math.max(nof_nodes, (int)Math.ceil(((double)fs.getContentSummary(job1_inputPath).getSpaceConsumed()/(double)fs.getDefaultReplication(job1_inputPath))/((double)fs.getDefaultBlockSize(job1_inputPath))));

		job1.setNumReduceTasks(job1_nof_chunks);

		job1.setPartitionerClass(PreprocessPartitioner.class);
		job1.setGroupingComparatorClass(PreprocessGroupingComparator.class);
		job1.setMapperClass(PreprocessMapper.class);
		job1.setReducerClass(PreprocessReducer.class);

		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapOutputKeyClass(DTJrPointElement.class);
		job1.setMapOutputValueClass(Text.class);

	
		FileInputFormat.setInputPaths(job1, job1_inputPath);
		FileOutputFormat.setOutputPath(job1, job1_outputPath);

		job1.waitForCompletion(true);
		
		Job job2 = Job.getInstance(conf, "Repartition");
		
    	if(fs.exists(new Path(workspace_dir.concat(input_dir)))){
    		fs.delete(new Path(workspace_dir.concat(input_dir)), true);
    	}
    	
		job2.setJarByClass(PreprocessDriver.class);
        
        Path job2_inputPath = new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(prep_dir));
        Path job2_partitionOutputPath = new Path(hostname.concat(dfs_port).concat(workspace_dir).concat("/TotalOrderPartitioner"));
        Path job2_outputPath = new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(input_dir));
     	
	    
        int job2_nof_chunks = Math.max(nof_nodes, (int)Math.ceil(((double)fs.getContentSummary(job2_inputPath).getSpaceConsumed()/(double)fs.getDefaultReplication(job2_inputPath))/((double)fs.getDefaultBlockSize(job2_inputPath))));

        job2.setNumReduceTasks(job2_nof_chunks);
        FileInputFormat.setInputPaths(job2, job2_inputPath);
        TotalOrderPartitioner.setPartitionFile(job2.getConfiguration(), job2_partitionOutputPath);
        job2.setInputFormatClass(KeyValueIntWritableInputFormat.class);
        
        job2.setMapOutputKeyClass(IntWritable.class);
        
        // Write partition file with interval sampler
        InputSampler.Sampler<IntWritable, Text> job2_sampler = new InputSampler.RandomSampler<IntWritable, Text>(sample_freq, max_nof_samples);
        InputSampler.writePartitionFile(job2, job2_sampler);
        
        // add partition file to distributed cache
        job2.addCacheFile(new URI(hostname.concat(dfs_port).concat(workspace_dir).concat("/TotalOrderPartitioner")));

        job2.setPartitionerClass(TotalOrderPartitioner.class);
        job2.setMapperClass(Mapper.class);
        job2.setReducerClass(EquiDepthHistReducer.class);
        
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job2, job2_outputPath);
        
        job2.waitForCompletion(true);

    	if(fs.exists(new Path(workspace_dir.concat("/DatasetMBB")))){
    		fs.delete(new Path(workspace_dir.concat("/DatasetMBB")), true);
    	}
    	
		Job job3 = Job.getInstance(conf, "DatasetMBB");
		
		job3.setNumReduceTasks(1);
		
		job3.setJarByClass(PreprocessDriver.class);

		job3.setMapperClass(DatasetMBBMapper.class);
		
		job3.setCombinerClass(DatasetMBBCombiner.class);

		job3.setReducerClass(DatasetMBBReducer.class);

		job3.setInputFormatClass(TextInputFormat.class);
		
		job3.setOutputKeyClass(NullWritable.class);
		job3.setOutputValueClass(Text.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
	
		FileInputFormat.setInputPaths(job3, new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(input_dir)));
		
		FileOutputFormat.setOutputPath(job3, new Path(hostname.concat(dfs_port).concat(workspace_dir).concat("/DatasetMBB")));

		job3.waitForCompletion(true);
        
		Path DMBB_path = new Path(hostname.concat(dfs_port).concat(workspace_dir).concat("/DatasetMBB/part-r-00000"));
        
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(DMBB_path)));
        String line;
        line=br.readLine();

    	StringTokenizer linetokenizer = new StringTokenizer(line, ",");
		int n_of_args = linetokenizer.countTokens();
		if (n_of_args == 6){
			while (linetokenizer.hasMoreTokens()) {
				dMBB = new String[6];

				dMBB[0] = linetokenizer.nextToken();
				dMBB[1] = linetokenizer.nextToken();
				dMBB[2] = linetokenizer.nextToken();
				dMBB[3] = linetokenizer.nextToken();
				dMBB[4] = linetokenizer.nextToken();
				dMBB[5] = linetokenizer.nextToken();
			}
		} else if (n_of_args == 8){
			while (linetokenizer.hasMoreTokens()) {
				dMBB = new String[8];

				dMBB[0] = linetokenizer.nextToken();
				dMBB[1] = linetokenizer.nextToken();
				dMBB[2] = linetokenizer.nextToken();
				dMBB[3] = linetokenizer.nextToken();
				dMBB[4] = linetokenizer.nextToken();
				dMBB[5] = linetokenizer.nextToken();
				dMBB[6] = linetokenizer.nextToken();
				dMBB[7] = linetokenizer.nextToken();
			}
		
		}
		
		conf.setStrings("DatasetMBB", dMBB);


        Job job4 = Job.getInstance(conf, "OctTree");
				
        Path job4_inputPath = new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(prep_dir));
        Path job4_partitionOutputPath = new Path(hostname.concat(dfs_port).concat(workspace_dir).concat("/octtree"));


        job4.setJarByClass(PreprocessDriver.class);
        
        job4.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job4, job4_inputPath);
        TotalOrderPartitioner.setPartitionFile(job4.getConfiguration(), job4_partitionOutputPath);
        job4.setInputFormatClass(KeyValueTextInputFormat.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setOutputFormatClass(NullOutputFormat.class);
        
        CreateOctTreePartitionFile.RandomSampler<IntWritable, Text> job4_sampler = new CreateOctTreePartitionFile.RandomSampler<IntWritable, Text>(sample_freq, max_nof_samples);
        CreateOctTreePartitionFile.writePartitionFile(job4, job4_sampler);
        
		job4.waitForCompletion(true);


    }

}
