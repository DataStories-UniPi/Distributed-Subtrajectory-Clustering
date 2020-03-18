package DSC;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MyBloatFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import CustomWritables.DTJSubtrajPointPair;
import CustomWritables.DTJrPairElement;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.StringTokenizer;


public class DTJiDriver {

	public static String hostname = new String();
	public static String dfs_port = new String();
	public static String rm_port = new String();
	public static String workspace_dir = new String();
	public static String dtjmr_input_dir = new String();
	public static String dtjmr_output_dir = new String();
	public static String dtjmr_subtraj_dir = new String();

	public static int nof_reducers;
	
	public static int e_sp_method;
	public static double epsilon_sp_prcnt;
	public static int epsilon_t;
	public static int dt;
	public static int SegmentationAlgorithm;
	public static int w;
	public static double tau;

	public static String[] dMBB;

    public static void main(String[] args) throws Exception {
    	
       
     	hostname = args[0];
     	dfs_port = args[1];
     	rm_port = args[2];
     	workspace_dir = args[3];
     	dtjmr_input_dir = args[4];
     	dtjmr_output_dir = args[5];
     	dtjmr_subtraj_dir = args[6];
     	
     	nof_reducers = Integer.parseInt(args[7]);
     	
     	e_sp_method = Integer.parseInt(args[8]);//1 for distance and 2 for percentage of the quadtree cell
     	epsilon_sp_prcnt = Double.parseDouble(args[9]);

     	epsilon_t = Integer.parseInt(args[10]);
     	dt = Integer.parseInt(args[11]);

     	SegmentationAlgorithm = Integer.parseInt(args[12]);
     	w = Integer.parseInt(args[13]);
     	tau = Double.parseDouble(args[14]);

     	final String job_description = args[15];
        
     	Path DMBB_path=new Path(hostname.concat(dfs_port).concat(workspace_dir).concat("/DatasetMBB/part-r-00000"));
    	
		Configuration conf = new Configuration();

     	FileSystem fs = FileSystem.get(conf);
        
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
       	conf.set("dtjmr_input_dir", dtjmr_input_dir);
       	conf.set("dtjmr_output_dir", dtjmr_output_dir);
       	conf.set("dtjmr_subtraj_dir", dtjmr_subtraj_dir);
       	conf.setInt("nof_reducers", nof_reducers);
       	
       	conf.setInt("e_sp_method", e_sp_method);

       	conf.setDouble("epsilon_sp_prcnt", epsilon_sp_prcnt);

       	conf.setInt("epsilon_t", epsilon_t);
       	conf.setInt("dt", dt);
       	conf.setInt("SegmentationAlgorithm", SegmentationAlgorithm);
       	conf.setInt("w", w);
       	conf.setDouble("tau", tau);
		
       	conf.setStrings("DatasetMBB", dMBB);
  	
       	conf.set("fs.default.name", hostname.concat(dfs_port));
       	
       	conf.setBoolean("dfs.support.append", true);
       	conf.set("mapreduce.task.io.sort.factor", "100");
    	conf.set("mapreduce.task.io.sort.mb", "64");
    	conf.set("mapreduce.map.sort.spill.percent", "0.9");
       	conf.set("mapreduce.job.reduce.slowstart.completedmaps", "1.00");

    	conf.set("mapreduce.jobtracker.address", "local");
       	conf.set("mapreduce.framework.name", "yarn");
       	conf.set("yarn.resourcemanager.address", hostname.concat(rm_port));

       	conf.set("mapreduce.map.java.opts", "-Xmx720m");//75%
       	conf.set("mapreduce.reduce.java.opts", "-Xmx720m");//75%
       	conf.set("mapred.child.java.opts", "-Xmx960m");
       	conf.set("mapreduce.map.memory.mb", "960");
       	conf.set("mapreduce.reduce.memory.mb", "960");
       	
       	conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.Lz4Codec");
        
    	conf.set("mapreduce.reduce.shuffle.input.buffer.percent", "0.2");
        
    	if(fs.exists(new Path(workspace_dir.concat(dtjmr_output_dir)))){
    		fs.delete(new Path(workspace_dir.concat(dtjmr_output_dir)), true);
    	}
    	
    	
    	if(fs.exists(new Path(workspace_dir.concat(dtjmr_subtraj_dir)))){
    		fs.delete(new Path(workspace_dir.concat(dtjmr_subtraj_dir)), true);
    	}

		Job job = Job.getInstance(conf, job_description);
		
		job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);//avoid running multiple attempts of the same task
		
		job.setNumReduceTasks(nof_reducers);
		
		job.setJarByClass(DTJiDriver.class);
		job.setPartitionerClass(DTJrPartitioner.class);
		job.setGroupingComparatorClass(DTJrGroupingComparator.class);
		job.setMapperClass(DTJiMapper.class);

		job.setReducerClass(DTJ2ClustReducer.class);


		job.setInputFormatClass(MyBloatFileInputFormat.class);

		job.setOutputKeyClass(DTJSubtrajPointPair.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(DTJrPairElement.class);
		job.setMapOutputValueClass(Text.class);
		
		job.addCacheFile(new URI(hostname.concat(dfs_port).concat(workspace_dir).concat("/octtree")));
		job.addCacheFile(new URI(hostname.concat(dfs_port).concat(workspace_dir).concat("/TotalOrderPartitioner")));

		MyBloatFileInputFormat.setInputPaths(job, new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(dtjmr_input_dir)));

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(hostname.concat(dfs_port).concat(workspace_dir).concat(dtjmr_output_dir)));

		job.waitForCompletion(true);

    }
}

