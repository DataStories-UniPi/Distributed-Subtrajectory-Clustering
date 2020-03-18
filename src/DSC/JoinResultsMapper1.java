package DSC;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import CustomWritables.DTJSubtraj;
import CustomWritables.DTJSubtrajValue;
import DataTypes.Period;
import DataTypes.PointSP;
import DataTypes.PointST;




public class JoinResultsMapper1 extends Mapper<LongWritable, Text, DTJSubtraj, Text> {
	
	int obj_id = 0;
	int traj_id = 0;
	int subtraj_id = 0;
	
	int mint = 0;
	int maxt = 0;
	int n_of_points = 0;
	double sum_voting = (double)0;
	
	DTJSubtraj subtraj_key = new DTJSubtraj();
	DTJSubtrajValue subtraj_value = new DTJSubtrajValue();
	HashMap<DTJSubtraj, DTJSubtrajValue> subtraj = new HashMap<DTJSubtraj, DTJSubtrajValue>();

	String line = new String();
	StringTokenizer linetokenizer = new StringTokenizer(line, ",");
    int r_obj_id = 0; 
    int r_traj_id = 0;

    int t = 0;
    int x = 0;
    int y = 0;
    int n_of_args = 0;
    
    PointST point = new PointST();
    DTJSubtraj subtrajElement = new DTJSubtraj();
    
    public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		line = ivalue.toString();

        linetokenizer = new StringTokenizer(line, ",");
		n_of_args = linetokenizer.countTokens();
		
		if (n_of_args == 5){
			while (linetokenizer.hasMoreTokens()) {
				r_obj_id = Integer.parseInt(linetokenizer.nextToken());
				r_traj_id = Integer.parseInt(linetokenizer.nextToken());
				point = new PointST(Integer.parseInt(linetokenizer.nextToken()), new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken())));
			}
			
		} else if (n_of_args == 6) {
			while (linetokenizer.hasMoreTokens()) {
				r_obj_id = Integer.parseInt(linetokenizer.nextToken());
				r_traj_id = Integer.parseInt(linetokenizer.nextToken());
				point = new PointST(Integer.parseInt(linetokenizer.nextToken()), new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken())));
			}
		}
        
    }
    
	@Override
	public void run(Context context) throws IOException, InterruptedException {

		setup(context);
	    Configuration conf = context.getConfiguration();
        FileSystem fs;

	    //Hack to get FileSplit
        ////////////////////////////////////////////////////////////////////
	    InputSplit split = context.getInputSplit();
	    Class<? extends InputSplit> splitClass = split.getClass();

	    FileSplit fileSplit = null;
	    if (splitClass.equals(FileSplit.class)) {
	        fileSplit = (FileSplit) split;
	    } else if (splitClass.getName().equals(
	            "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
	        // begin reflection hackery...

	        try {
	            Method getInputSplitMethod = splitClass.getDeclaredMethod("getInputSplit");
	            getInputSplitMethod.setAccessible(true);
	            fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
	        } catch (Exception e) {
	            // wrap and re-throw error
	            throw new IOException(e);
	        }

	        // end reflection hackery
	    }
	    ////////////////////////////////////////////////////////////////////
	    
        String filename = fileSplit.getPath().getName();
        
        String[] trim1 = filename.split("-");
        String minTmaxT = trim1[0];
        String[] trim2 = minTmaxT.split(" ");
        int start = Integer.parseInt(trim2[0]);
        int end = Integer.parseInt(trim2[1]);
		
		Period part_period = new Period(start, end);
		
	    String workspace = conf.get("workspace_dir");
        String subtraj_dir = conf.get("subtraj_dir");

	    
		Path input_path = new Path(workspace.concat(subtraj_dir));
		RemoteIterator<LocatedFileStatus> iter = input_path.getFileSystem(conf).listFiles(input_path, true);
		
		while (iter.hasNext()) {

			Path file_path = iter.next().getPath();
            fs = file_path.getFileSystem(conf);
            FSDataInputStream subtrajFile = fs.open(file_path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(subtrajFile));
            String line;
            
            while ((line = reader.readLine()) != null){
            	
                linetokenizer = new StringTokenizer(line, ",");

        		while (linetokenizer.hasMoreTokens()) {

        			obj_id = Integer.parseInt(linetokenizer.nextToken());
        			traj_id = Integer.parseInt(linetokenizer.nextToken());
        			subtraj_id = Integer.parseInt(linetokenizer.nextToken());
        			mint = Integer.parseInt(linetokenizer.nextToken());
        			maxt = Integer.parseInt(linetokenizer.nextToken());
        			n_of_points = Integer.parseInt(linetokenizer.nextToken());
        			sum_voting = Double.parseDouble(linetokenizer.nextToken());
        		}
        		

        		if (part_period.IntersectsPeriod(new Period(mint, maxt))){

	        		subtraj_key = new DTJSubtraj(obj_id, traj_id, subtraj_id);
	        		subtraj_value = new DTJSubtrajValue(mint, maxt, n_of_points, sum_voting);
	        		subtraj.put(subtraj_key, subtraj_value);
        		
        		}
        		
            }
            reader.close();
        }
        
        
		while (context.nextKeyValue()) {
			
			map(context.getCurrentKey(), context.getCurrentValue(), context);
			
			int subtraj_cnt = 1;

			while(true){
				
		        
		        if (subtraj.containsKey(new DTJSubtraj(r_obj_id, r_traj_id, subtraj_cnt))){

					if(point.t >= subtraj.get(new DTJSubtraj(r_obj_id, r_traj_id, subtraj_cnt)).mint && point.t <= subtraj.get(new DTJSubtraj(r_obj_id, r_traj_id, subtraj_cnt)).maxt){
						
						subtrajElement = new DTJSubtraj(r_obj_id, r_traj_id, subtraj_cnt);
						break;

					} 
		        	
		        } else {
		    		
		        	break;
		        	
		        }
				
				subtraj_cnt++;
			}
			
			context.write(subtrajElement, new Text("Mapper1," + point.toString()));
			
		}
		
		cleanup(context);
	 
	}

}
