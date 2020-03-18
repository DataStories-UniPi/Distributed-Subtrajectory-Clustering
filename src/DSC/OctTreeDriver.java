package DSC;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.CreateOctTreePartitionFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;



public class OctTreeDriver {
	
	public static String hostname = new String();
	public static String dfs_port = new String();
	public static String rm_port = new String();
	public static String workspace_dir = new String();
	public static String prepr_points_dir = new String();
	public static Double maxCellPtsPrcnt = 0.0;
	public static double sample_freq;
	public static int max_nof_samples;

	public static String[] dMBB;

    public static void main(String[] args) throws Exception {
     	
    	hostname = args[0];
    	dfs_port = args[1];
     	rm_port = args[2];
     	workspace_dir = args[3];
     	prepr_points_dir = args[4];
     	maxCellPtsPrcnt = Double.parseDouble(args[5]);
     	sample_freq = Double.parseDouble(args[6]);
     	max_nof_samples = Integer.parseInt(args[7]);

        Path DMBB_path = new Path(hostname.concat(dfs_port).concat(workspace_dir).concat("/DatasetMBB/part-r-00000"));
        
        Path inputPath = new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(prepr_points_dir));
        Path partitionOutputPath = new Path(hostname.concat(dfs_port).concat(workspace_dir).concat("/octtree"));

    	Configuration conf = new Configuration();
       
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(DMBB_path)));
        String line;
        line=br.readLine();
        System.out.println(line);
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
		
       	conf.setDouble("maxCellPtsPrcnt", maxCellPtsPrcnt);
		conf.setStrings("DatasetMBB", dMBB);
    	
       	conf.set("fs.default.name", hostname.concat(dfs_port));
    	conf.set("mapreduce.jobtracker.address", "local");
       	conf.set("mapreduce.framework.name", "yarn");
       	conf.set("yarn.resourcemanager.address", hostname.concat(rm_port));

        Job job = Job.getInstance(conf, "OctTree");
       	

        job.setJarByClass(OctTreeDriver.class);
        
        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, inputPath);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionOutputPath);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        
        CreateOctTreePartitionFile.RandomSampler<IntWritable, Text> sampler = new CreateOctTreePartitionFile.RandomSampler<IntWritable, Text>(sample_freq, max_nof_samples);
        CreateOctTreePartitionFile.writePartitionFile(job, sampler);
        
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
    }
}
