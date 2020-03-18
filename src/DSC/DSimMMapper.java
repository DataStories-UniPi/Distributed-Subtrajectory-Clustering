package DSC;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import CustomWritables.DTJSubtraj;
import CustomWritables.DTJSubtrajPair;
import CustomWritables.DTJSubtrajPointPair;
import CustomWritables.DTJSubtrajRight;
import CustomWritables.DTJSubtrajSim;
import CustomWritables.DTJSubtrajValue;



public class DSimMMapper extends Mapper<LongWritable, Text, DTJSubtraj, List<DTJSubtrajSim>> {
	
	String hostname = new String();
	String dfs_port = new String();
	String workspace = new String();
    String subtraj_dir = new String();

	String key_value = new String();
	StringTokenizer key_valuetokenizer = new StringTokenizer(key_value, "\t");
	String key = new String();
	StringTokenizer keytokenizer = new StringTokenizer(key, ",");
	String value = new String();
	StringTokenizer valuetokenizer = new StringTokenizer(value, ", ");
	String element = new String();
	StringTokenizer elementtokenizer = new StringTokenizer(value, ",");

	int r_obj_id;
	int r_traj_id;
	int r_subtraj_id;
	int s_obj_id;
	int s_traj_id;
	
	int r_t;

	int s_t;
	double sim;

	int obj_id = 0;
	int traj_id = 0;
	int subtraj_id = 0;
	
	int mint = 0;
	int maxt = 0;
	int n_of_points = 0;
	double sum_voting = (double)0;
	
	double avg_sim = (double)0;
	String[] trim;
	int reducer_id;
	
	double sum_similarity = (double)0;
	int sim_count = 0;

	DTJSubtraj subtraj_key = new DTJSubtraj();
	DTJSubtrajValue subtraj_value = new DTJSubtrajValue();

	HashMap<DTJSubtraj, DTJSubtrajValue> subtraj = new HashMap<DTJSubtraj, DTJSubtrajValue>();
	HashMap<DTJSubtraj, DTJSubtrajValue> split_subtraj = new HashMap<DTJSubtraj, DTJSubtrajValue>();

	HashMap<DTJSubtrajPair, ArrayList<DTJSubtrajRight>> subtrajSimilarity_tmp = new HashMap<DTJSubtrajPair, ArrayList<DTJSubtrajRight>>();
	HashMap<DTJSubtraj, HashSet<DTJSubtrajSim>> subtrajSimilarity = new HashMap<DTJSubtraj, HashSet<DTJSubtrajSim>>();
	ArrayList<DTJSubtrajRight> val_array = new ArrayList<DTJSubtrajRight>();

	DTJSubtraj output_key = new DTJSubtraj();
	List<DTJSubtrajSim> output_value = new ArrayList<DTJSubtrajSim>();
	
	HashMap<Integer, Double> distinct_points = new HashMap<Integer, Double>();

	
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		DTJSubtrajPointPair key_pair = new DTJSubtrajPointPair();
		
		val_array.clear();
		

		key_value = ivalue.toString();
		key_valuetokenizer = new StringTokenizer(key_value, "\t");
		while (key_valuetokenizer.hasMoreTokens()) {
			key = key_valuetokenizer.nextToken();
			value = key_valuetokenizer.nextToken();
		}
		
		keytokenizer = new StringTokenizer(key, ",");
		while (keytokenizer.hasMoreTokens()) {
			r_obj_id = Integer.parseInt(keytokenizer.nextToken());
			r_traj_id = Integer.parseInt(keytokenizer.nextToken());
			r_subtraj_id = Integer.parseInt(keytokenizer.nextToken());
			s_obj_id = Integer.parseInt(keytokenizer.nextToken());
			s_traj_id = Integer.parseInt(keytokenizer.nextToken());
		}
		
		key_pair = new DTJSubtrajPointPair(r_obj_id, r_traj_id, r_subtraj_id, s_obj_id, s_traj_id);
				
		value = value.replace("[", "");
		value = value.replace("]", "");

		valuetokenizer = new StringTokenizer(value, ",");
		while (valuetokenizer.hasMoreTokens()) {
			r_t = Integer.parseInt(valuetokenizer.nextToken().trim());

			s_t = Integer.parseInt(valuetokenizer.nextToken().trim());
			sim = Double.parseDouble(valuetokenizer.nextToken());

			DTJSubtrajRight val = new DTJSubtrajRight(r_t, s_t,sim);

			val_array.add(val);
		}
		
		subtrajSimilarity_tmp.clear();
		

		for (int i = 0; i < val_array.size(); i++){
			
			int subtraj_id = 1;
			
			while (true){

				DTJSubtraj right_subtraj = new DTJSubtraj(key_pair.s_obj_id, key_pair.s_traj_id, subtraj_id);
				DTJSubtrajPair subtraj_pair = new DTJSubtrajPair(key_pair.r_obj_id, key_pair.r_traj_id, key_pair.r_subtraj_id, key_pair.s_obj_id, key_pair.s_traj_id, subtraj_id);
				
				if (subtraj.containsKey(right_subtraj)){
					
					if (val_array.get(i).s_t >= subtraj.get(right_subtraj).mint && val_array.get(i).s_t <= subtraj.get(right_subtraj).maxt){
						
						if (!subtrajSimilarity_tmp.containsKey(subtraj_pair)){
							
							ArrayList<DTJSubtrajRight> l = new ArrayList<DTJSubtrajRight>();
							DTJSubtrajRight r = new DTJSubtrajRight(val_array.get(i).r_t, val_array.get(i).s_t, val_array.get(i).sim);
							l.add(r);
							subtrajSimilarity_tmp.put(subtraj_pair, l);

						} else {
							
							DTJSubtrajRight r = new DTJSubtrajRight(val_array.get(i).r_t, val_array.get(i).s_t, val_array.get(i).sim);
							subtrajSimilarity_tmp.get(subtraj_pair).add(r);

						}
						
						break;

					}
					
				} else {
					
					break;
					
				}
				
				subtraj_id++;
				
			}
		}

		for (Map.Entry<DTJSubtrajPair, ArrayList<DTJSubtrajRight>> entry_ss : subtrajSimilarity_tmp.entrySet()){

			double sum_voting = (double)0;
			double similarity = (double)0;

			distinct_points.clear();

			DTJSubtraj l_subtraj = new DTJSubtraj(entry_ss.getKey().r_obj_id, entry_ss.getKey().r_traj_id, entry_ss.getKey().r_subtraj_id);


			int l_n_of_points = subtraj.get(l_subtraj).n_of_points;

			DTJSubtraj r_subtraj = new DTJSubtraj(entry_ss.getKey().s_obj_id, entry_ss.getKey().s_traj_id, entry_ss.getKey().s_subtraj_id);
			int r_n_of_points = subtraj.get(r_subtraj).n_of_points;
			
			
			for (int i = 0; i < entry_ss.getValue().size(); i++){
				if (!distinct_points.containsKey(entry_ss.getValue().get(i).s_t)){
					sum_voting = sum_voting + entry_ss.getValue().get(i).sim;
					distinct_points.put(entry_ss.getValue().get(i).s_t, entry_ss.getValue().get(i).sim);
				} else {
					if (entry_ss.getValue().get(i).sim > distinct_points.get(entry_ss.getValue().get(i).s_t)){
						sum_voting = sum_voting - distinct_points.get(entry_ss.getValue().get(i).s_t) + entry_ss.getValue().get(i).sim;
						distinct_points.put(entry_ss.getValue().get(i).s_t, entry_ss.getValue().get(i).sim);
					}
				}

			}
			
			similarity = sum_voting/Math.min(l_n_of_points, r_n_of_points);
			
			sum_similarity += similarity;
			sim_count++;
			
			DTJSubtrajSim r_subtrajSim = new DTJSubtrajSim(r_subtraj.obj_id, r_subtraj.traj_id, r_subtraj.subtraj_id, similarity);
			
			if (!subtrajSimilarity.containsKey(l_subtraj)){
				
				HashSet<DTJSubtrajSim> l = new HashSet<DTJSubtrajSim>();
				l.add(r_subtrajSim);
				subtrajSimilarity.put(l_subtraj, l);
				
			} else {
				
				subtrajSimilarity.get(l_subtraj).add(r_subtrajSim);

			}
			
		}

	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {

		setup(context);
		
        Configuration conf = context.getConfiguration();
        
        hostname = conf.get("hostname");
        dfs_port = conf.get("dfs_port");

        workspace = conf.get("workspace_dir");
        subtraj_dir = conf.get("subtraj_dir");



        FileSystem fs;

    	StringTokenizer linetokenizer = new StringTokenizer(key_value, ",");
		trim = context.getInputSplit().toString().split(":");
		reducer_id = Integer.parseInt(trim[2].split("part-r-")[1].replaceFirst("^0+(?!$)", ""));
		Path split_path = new Path(hostname.concat(dfs_port).concat(workspace).concat(subtraj_dir).concat("/subtraj-" + reducer_id));
		
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

    			
        		subtraj_key = new DTJSubtraj(obj_id, traj_id, subtraj_id);
        		subtraj_value = new DTJSubtrajValue(mint, maxt, n_of_points, sum_voting);
        		subtraj.put(subtraj_key, subtraj_value);
        		
        		if(file_path.toUri().equals(split_path.toUri())){
            		
        			split_subtraj.put(subtraj_key, subtraj_value);
        			
        		}
        		
            }
		
            reader.close();

			
		}

		while (context.nextKeyValue()) {
			
			map(context.getCurrentKey(), context.getCurrentValue(), context);
			
		}
		
        for(Map.Entry<DTJSubtraj, DTJSubtrajValue> entry_ss : split_subtraj.entrySet()){
        	
    		output_key = new DTJSubtraj(entry_ss.getKey());

        	if(subtrajSimilarity.containsKey(entry_ss.getKey())){
    			
    			output_value = new ArrayList<DTJSubtrajSim>(subtrajSimilarity.get(entry_ss.getKey()));
        		
        	} else {
        		
    			output_value = new ArrayList<DTJSubtrajSim>();
        		
        	}
        	
			context.write(output_key, output_value);
        	
        }

  
		cleanup(context);
	 
	}

	
}
