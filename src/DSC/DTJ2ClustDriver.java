package DSC;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MyMergePartitionFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import CustomWritables.DTJSubtraj;
import CustomWritables.DTJSubtrajIntersectPart;
import CustomWritables.DTJSubtrajPairSim;
import CustomWritables.DTJSubtrajSim;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;


public class DTJ2ClustDriver {
	public static String hostname = new String();
	public static String dfs_port = new String();
	public static String rm_port = new String();
	public static String workspace_dir = new String();
	public static String dtjmr_input_dir = new String();
	public static String dtjmr_output_dir = new String();
	public static String dtjmr_subtraj_dir = new String();
	public static String dtj2clust_output_dir = new String();

	public static int nof_reducers;
	public static int nof_chunks;

	public static int epsilon_sp;
	public static int epsilon_t;
	public static int dt;
	public static int SegmentationAlgorithm;
	public static int w;
	public static double tau;
	public static double threshSim;
	public static double minVoting;
	public static double s;

	public static String[] dMBB;
	
    public static void main(String[] args) throws Exception {
    	
       
     	hostname = args[0];
     	dfs_port = args[1];
     	rm_port = args[2];
     	workspace_dir = args[3];
     	dtjmr_output_dir = args[4];
     	dtjmr_subtraj_dir = args[5];
     	dtj2clust_output_dir = args[6];
     	
     	nof_reducers = Integer.parseInt(args[7]);

     	threshSim = Double.parseDouble(args[8]);
     	minVoting = Double.parseDouble(args[9]);

     	final String job_description = args[10];
     	
		Configuration conf = new Configuration();

     	FileSystem fs = FileSystem.get(conf);

        Path DMBB_path=new Path(hostname.concat(dfs_port).concat(workspace_dir).concat("/DatasetMBB/part-r-00000"));
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

		
       	conf.set("hostname", hostname);
       	conf.set("dfs_port", dfs_port);
       	conf.set("rm_port", rm_port);
       	conf.set("workspace_dir", workspace_dir);
       	conf.set("dtjmr_output_dir", dtjmr_output_dir);
       	conf.set("dtjmr_subtraj_dir", dtjmr_subtraj_dir);
       	conf.set("dtj2clust_output_dir", dtj2clust_output_dir);
       	conf.setInt("nof_reducers", nof_reducers);
       	conf.setDouble("threshSim", threshSim);
       	conf.setDouble("minVoting", minVoting);
		conf.setStrings("DatasetMBB", dMBB);

       	conf.set("fs.default.name", hostname.concat(dfs_port));
       	
       	conf.setBoolean("dfs.support.append", true);

    	conf.set("mapreduce.jobtracker.address", "local");
       	conf.set("mapreduce.framework.name", "yarn");
       	conf.set("yarn.resourcemanager.address", hostname.concat(rm_port));

       	
       	conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.Lz4Codec");
    	
    	if(fs.exists(new Path(workspace_dir.concat(dtj2clust_output_dir)))){
    		fs.delete(new Path(workspace_dir.concat(dtj2clust_output_dir)), true);
    	}

 
		Job job = Job.getInstance(conf, job_description);
		
		job.setNumReduceTasks(nof_reducers);
		
		job.setJarByClass(DTJ2ClustDriver.class);
		job.setPartitionerClass(DTJ2ClustPartitioner.class);
		job.setGroupingComparatorClass(DTJ2ClustGroupingComparator.class);
		
		job.setMapperClass(DTJ2ClustMapper.class);

		job.setReducerClass(DTJ2ClustRefineReducer.class);


		job.setInputFormatClass(MyMergePartitionFileInputFormat.class);

		

		// TODO: specify output types
		job.setMapOutputKeyClass(DTJSubtrajIntersectPart.class);
		job.setMapOutputValueClass(DTJSubtrajPairSim.class);
		job.setOutputKeyClass(DTJSubtraj.class);
		job.setOutputValueClass(DTJSubtrajSim.class);


		// TODO: specify input and output DIRECTORIES (not files)
	
		MyMergePartitionFileInputFormat.setInputPaths(job, new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(dtjmr_output_dir)));
		
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(dtj2clust_output_dir)));

		job.waitForCompletion(true);

    }
}

