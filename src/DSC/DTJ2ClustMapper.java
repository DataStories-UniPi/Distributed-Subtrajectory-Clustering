package DSC;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeSet;

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
import CustomWritables.DTJSubtrajIntersectPart;
import CustomWritables.DTJSubtrajPair;
import CustomWritables.DTJSubtrajPairSim;
import CustomWritables.DTJSubtrajPointPair;
import CustomWritables.DTJSubtrajRight;
import CustomWritables.DTJSubtrajSim;
import CustomWritables.DTJSubtrajValue;
import CustomWritables.DTJSubtrajVot;
import DataTypes.BoxST;
import DataTypes.Period;
import DataTypes.PointSP;
import DataTypes.PointST;

public class DTJ2ClustMapper extends Mapper<LongWritable, Text, DTJSubtrajIntersectPart, DTJSubtrajPairSim> {
    
	BoxST DatasetMBB = new BoxST();
	double minVoting;
	double threshSim;
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
	
	double sum_similarity = (double)0;
	int sim_count = 0;
	double avg_sim = (double)0;
	String[] trim;
	int start;
	int end;
	
	
	DTJSubtraj subtraj_key = new DTJSubtraj();
	DTJSubtrajValue subtraj_value = new DTJSubtrajValue();
	DTJSubtrajVot subtraj_vot = new DTJSubtrajVot();
	TreeSet<DTJSubtrajVot> subtraj_vot_set = new TreeSet<DTJSubtrajVot>(Collections.reverseOrder());
	HashMap<DTJSubtraj, HashSet<DTJSubtrajSim>> Clusters = new HashMap<DTJSubtraj, HashSet<DTJSubtrajSim>>();
	HashSet<DTJSubtraj> Outliers = new HashSet<DTJSubtraj>();
	DTJSubtraj CandidateRepr = new DTJSubtraj();
	boolean assigned_toCluster;
	double max_sim = (double)0;
	
	HashMap<DTJSubtraj, DTJSubtrajValue> subtraj = new HashMap<DTJSubtraj, DTJSubtrajValue>();

	HashMap<DTJSubtrajPair, ArrayList<DTJSubtrajRight>> subtrajSimilarity_tmp = new HashMap<DTJSubtrajPair, ArrayList<DTJSubtrajRight>>();
	HashMap<DTJSubtraj, HashSet<DTJSubtrajSim>> subtrajSimilarity = new HashMap<DTJSubtraj, HashSet<DTJSubtrajSim>>();
	ArrayList<DTJSubtrajRight> val_array = new ArrayList<DTJSubtrajRight>();

	DTJSubtrajIntersectPart output_key = new DTJSubtrajIntersectPart();
	DTJSubtrajPairSim output_value = new DTJSubtrajPairSim();
	
	HashMap<Integer, Double> distinct_points = new HashMap<Integer, Double>();

	int intersects = 0;
	
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
        
        workspace = conf.get("workspace_dir");
        subtraj_dir = conf.get("subtraj_dir");

        minVoting = conf.getDouble("minVoting", 0);
        threshSim = conf.getDouble("threshSim", 0);
        
        double total_sum_voting = 0;
        double total_avg_voting = 0;
		
        String[] arr = conf.getStrings("DatasetMBB");
        
        if (arr.length == 6){
			
        	DatasetMBB = new BoxST(new PointST(Integer.parseInt(arr[0]), new PointSP(Integer.parseInt(arr[1]),Integer.parseInt(arr[2]))), new PointST(Integer.parseInt(arr[3]), new PointSP(Integer.parseInt(arr[4]),Integer.parseInt(arr[5]))));

        } else if (arr.length == 8){
        	
        	DatasetMBB = new BoxST(new PointST(Integer.parseInt(arr[0]), new PointSP(Integer.parseInt(arr[1]),Integer.parseInt(arr[2]),Integer.parseInt(arr[3]))), new PointST(Integer.parseInt(arr[4]), new PointSP(Integer.parseInt(arr[5]),Integer.parseInt(arr[6]),Integer.parseInt(arr[7]))));

        }

        FileSystem fs;

    	StringTokenizer linetokenizer = new StringTokenizer(key_value, ",");
		trim = context.getInputSplit().toString().split(",");
		start = Integer.parseInt(trim[0].split(":")[1]);
		end = Integer.parseInt(trim[1].split(":")[1]);
		
		Period part_period = new Period(start, end);

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
            		subtraj_vot = new DTJSubtrajVot(obj_id, traj_id, subtraj_id, sum_voting/(double)n_of_points);
            		subtraj_vot_set.add(subtraj_vot);
            		
            		total_sum_voting = total_sum_voting + subtraj_vot.voting;
        		}
            }
            reader.close();

			
		}

		while (context.nextKeyValue()) {
			
			map(context.getCurrentKey(), context.getCurrentValue(), context);
			
		}
		
		
		total_avg_voting = total_sum_voting/(double)subtraj_vot_set.size();
		

        double val = 0;
        double squrDiffToMean = 0;
        double squrDiff_sum = 0;

		Iterator<DTJSubtrajVot> stdv_iter;
        stdv_iter = subtraj_vot_set.iterator();
        
        while (stdv_iter.hasNext()) {
        	
        	val = stdv_iter.next().voting;
        	squrDiffToMean = Math.pow(val - total_avg_voting, 2);
        	squrDiff_sum += squrDiffToMean;
        	
        }
        
        double meanOfDiffs = (double) squrDiff_sum / (double) (subtraj_vot_set.size());
        double stdv_voting = Math.sqrt(meanOfDiffs);
        
        
        minVoting = total_avg_voting + (minVoting * stdv_voting);
        
        //Calculate Stddev of similarity
		avg_sim = sum_similarity/(double)sim_count;

        val = 0;
        squrDiffToMean = 0;
        squrDiff_sum = 0;
        meanOfDiffs = 0;
        int count = 0;
        
        for (Map.Entry<DTJSubtraj, HashSet<DTJSubtrajSim>> entry_ss : subtrajSimilarity.entrySet()){

			Iterator<DTJSubtrajSim> iter_ss;
			iter_ss = entry_ss.getValue().iterator();
	        
			while (iter_ss.hasNext()) {
	        
				val = iter_ss.next().sim;
	        	squrDiffToMean = Math.pow(val - avg_sim, 2);
	        	squrDiff_sum += squrDiffToMean;
	        	count++;
	        }
		
		}
        
        meanOfDiffs = (double) squrDiff_sum / (double)count;

        threshSim = avg_sim + (threshSim * stdv_voting);

         
     
        Iterator<DTJSubtrajVot> iterator;
        iterator = subtraj_vot_set.iterator();

		
        while (iterator.hasNext()) {
        	
        	DTJSubtrajVot current_subtraj = iterator.next();

        	max_sim = (double)0;
        	CandidateRepr = new DTJSubtraj(current_subtraj.obj_id, current_subtraj.traj_id, current_subtraj.subtraj_id);

        	
        	if (subtrajSimilarity.containsKey(CandidateRepr)){
        		
        		if (current_subtraj.voting > minVoting){
        			
	        		if (subtraj.get(CandidateRepr).clust_sim == (double)-1){//if not assigned to cluster
		        		
		        		HashSet<DTJSubtrajSim> l = new HashSet<DTJSubtrajSim>();
	        			Iterator<DTJSubtrajSim> iter_ss;
		        		iter_ss = subtrajSimilarity.get(CandidateRepr).iterator();
		        		

		        		while(iter_ss.hasNext()){
		        			
		        			DTJSubtrajSim ss = iter_ss.next();
		        			DTJSubtraj sss = ss.toDTJSubtraj();
		        			
		        			if(subtraj.get(sss).clust_sim == (double)-1){//not assigned to cluster
		        				
		        				if (ss.sim >= threshSim){
		        					
			        				l.add(ss);
			        				Outliers.remove(sss);
				        			subtraj.get(sss).setclust_sim(ss.sim);
				        			subtraj.get(sss).setrepr_subtraj(CandidateRepr);

		        				} else {
		        					
		        					Outliers.add(sss);
		        					
		        				}

		        			} else if(ss.sim > subtraj.get(sss).clust_sim){//assigned to cluster
		        				
		        				l.add(ss);
		        				Clusters.get(subtraj.get(sss).repr_subtraj).remove(sss);
			        			subtraj.get(sss).setclust_sim(ss.sim);
			        			subtraj.get(sss).setrepr_subtraj(CandidateRepr);
    				
		        			}
		        			
		        		}
			    			
		        		Clusters.put(CandidateRepr, l);

	        		}
			        			        	
		        	
        		} else {
	        		
	        		Outliers.add(CandidateRepr);
	        		
	        	}

        	}

        }
        
        ArrayList<DTJSubtraj> to_delete = new ArrayList<DTJSubtraj>();
        
		for (Map.Entry<DTJSubtraj, HashSet<DTJSubtrajSim>> entry_cl : Clusters.entrySet()){


			if(entry_cl.getValue().size() == 0){
				
				if (subtrajSimilarity.get(entry_cl.getKey()).size() > 0){
					
					DTJSubtrajSim to_assign = new DTJSubtrajSim();
	        		
	        		ArrayList<DTJSubtrajSim> cand_list = new ArrayList<DTJSubtrajSim>(subtrajSimilarity.get(entry_cl.getKey()));
	                
	        		Collections.sort(cand_list, new Comparator<DTJSubtrajSim>(){
	        			
	        			public int compare(DTJSubtrajSim o1, DTJSubtrajSim o2) {
	                        return Double.compare(o2.sim, o1.sim);
	                    }
	                    
	                });

	        		
	        		
	        		
	        		double max_sim = (double)0;
	    			DTJSubtrajSim sel_cl = new DTJSubtrajSim();
	    			
	    			for (int i = 0; i < cand_list.size(); i++){//sorted in descending order
	        			
	        			DTJSubtrajSim cand = cand_list.get(i);
	        			
	        			if(cand.sim >= threshSim){

	        				to_assign = new DTJSubtrajSim(entry_cl.getKey().obj_id, entry_cl.getKey().traj_id, entry_cl.getKey().subtraj_id, max_sim);
	        				sel_cl = new DTJSubtrajSim(cand.obj_id, cand.traj_id, cand.subtraj_id, cand.sim);
		        			
	        				if (Clusters.containsKey(sel_cl)){
		            			
			        			Clusters.get(sel_cl).add(to_assign);
			        			subtraj.get(to_assign).setclust_sim(max_sim);
			        			subtraj.get(to_assign).setrepr_subtraj(sel_cl.toDTJSubtraj());
			        			to_delete.add(entry_cl.getKey());			        			
			        			break;
			        			
		            		} 
	        				
	        			} else {
	        				
	        				Outliers.add(entry_cl.getKey());
	        				to_delete.add(entry_cl.getKey());
	        				break;
	        				
	        			}
	        			
	        			if (i == cand_list.size() -1){
	        				
	        				Outliers.add(entry_cl.getKey());
	        				to_delete.add(entry_cl.getKey());
	        			}
	        			
	        		}
	        		

				} else {
					
	        		Outliers.add(entry_cl.getKey());
	        		to_delete.add(entry_cl.getKey());
				}
				
				
			}
		}
		
        for(int i = 0; i < to_delete.size(); i++){
        	
        	Clusters.remove(to_delete.get(i));
        	
        }
        
        
        DTJSubtraj repr = new DTJSubtraj();
        
		for (Map.Entry<DTJSubtraj, HashSet<DTJSubtrajSim>> entry_cl : Clusters.entrySet()){
			
			repr = new DTJSubtraj(entry_cl.getKey().obj_id, entry_cl.getKey().traj_id, entry_cl.getKey().subtraj_id);
			
			//emit representative with itself
			
			if (subtraj.get(repr).mint < part_period.ti || subtraj.get(repr).maxt > part_period.te){
				
				output_key = new DTJSubtrajIntersectPart(-2,-2,-2, start);
			
			} else{
				
				output_key = new DTJSubtrajIntersectPart(repr.obj_id, repr.traj_id, repr.subtraj_id, start);

			}
			
			output_value = new DTJSubtrajPairSim(repr.obj_id, repr.traj_id, repr.subtraj_id, repr.obj_id, repr.traj_id, repr.subtraj_id, 1);
			
			context.write(output_key, output_value);
			
			//emit cluster members with representative
			Iterator<DTJSubtrajSim> iter_cl;
			iter_cl = entry_cl.getValue().iterator();
			while(iter_cl.hasNext()){
				
				DTJSubtrajSim cl_member = new DTJSubtrajSim(iter_cl.next());
				
				if (subtraj.get(cl_member.toDTJSubtraj()).mint < part_period.ti || subtraj.get(cl_member.toDTJSubtraj()).maxt > part_period.te){
					
					output_key = new DTJSubtrajIntersectPart(-2,-2,-2, start);
				
				} else{
					
					output_key = new DTJSubtrajIntersectPart(repr.obj_id, repr.traj_id, repr.subtraj_id, start);

				}
				

				output_value = new DTJSubtrajPairSim(repr.obj_id, repr.traj_id, repr.subtraj_id, cl_member.obj_id, cl_member.traj_id, cl_member.subtraj_id, cl_member.sim);

				context.write(output_key, output_value);
				
			}

		}
		
		
		Iterator<DTJSubtraj> iter_out;
		iter_out = Outliers.iterator();
		
		while(iter_out.hasNext()){

			DTJSubtraj out_member = new DTJSubtraj(iter_out.next());
			
			if (subtraj.get(out_member).mint < part_period.ti || subtraj.get(out_member).maxt > part_period.te){
				
				output_key = new DTJSubtrajIntersectPart(-2,-2,-2, start);
			
			} else{
				
				output_key = new DTJSubtrajIntersectPart(-1,-1,-1, start);

			}

			output_value = new DTJSubtrajPairSim(-1,-1,-1, out_member.obj_id, out_member.traj_id, out_member.subtraj_id, 0);

			context.write(output_key, output_value);

		}

		cleanup(context);
	 
	}

	
}
