package DSC;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import CustomWritables.DTJSubtraj;
import CustomWritables.DTJSubtrajPointPair;
import CustomWritables.DTJSubtrajRight;
import CustomWritables.DTJSubtrajValueShort;
import CustomWritables.DTJrPairElement;
import CustomWritables.DTJrPointElement;
import CustomWritables.DTJrPointElement2;
import DataTypes.BoxST;
import DataTypes.Period;
import DataTypes.PointSP;
import DataTypes.PointST;


public class DTJ2SimReducer extends Reducer<DTJrPairElement, Text, DTJSubtrajPointPair, List<DTJSubtrajRight>> {

	
    String workspace_dir = new String();
    String dtjmr_subtraj_dir = new String();
    int epsilon_t;
    int dt;
    int SegmentationAlgorithm;
    int w;
    int e_sp_method;
    double tau;
    int epsilon_sp;
    BoxST DatasetMBB = new BoxST();



    FSDataOutputStream fileOutputStream;
    PrintWriter writer;

	@Override
	public void setup(Context context) throws IOException {
		

	    Configuration conf = context.getConfiguration();
	    
        workspace_dir = conf.get("workspace_dir");
        dtjmr_subtraj_dir = conf.get("subtraj_dir");
        epsilon_t = Integer.parseInt(conf.get("epsilon_t"));
        dt = Integer.parseInt(conf.get("dt"));
        SegmentationAlgorithm = Integer.parseInt(conf.get("SegmentationAlgorithm"));
        w = Integer.parseInt(conf.get("w"));
        tau = Double.parseDouble(conf.get("tau"));
        e_sp_method = Integer.parseInt(conf.get("e_sp_method"));
    	
        if (e_sp_method == 1){
    		
	        epsilon_sp = (int)Math.round(Double.parseDouble(conf.get("epsilon_sp_prcnt")));	        	
	        	            
    	}

        String[] arr = conf.getStrings("DatasetMBB");


        if (arr.length == 6){
			
        	DatasetMBB = new BoxST(new PointST(Integer.parseInt(arr[0]), new PointSP(Integer.parseInt(arr[1]),Integer.parseInt(arr[2]))), new PointST(Integer.parseInt(arr[3]), new PointSP(Integer.parseInt(arr[4]),Integer.parseInt(arr[5]))));

        } else if (arr.length == 8){
        	
        	DatasetMBB = new BoxST(new PointST(Integer.parseInt(arr[0]), new PointSP(Integer.parseInt(arr[1]),Integer.parseInt(arr[2]),Integer.parseInt(arr[3]))), new PointST(Integer.parseInt(arr[4]), new PointSP(Integer.parseInt(arr[5]),Integer.parseInt(arr[6]),Integer.parseInt(arr[7]))));

        }

  
	    
	    int reducerID = context.getTaskAttemptID().getTaskID().getId();

	    FileSystem fileSystem = FileSystem.get(conf);
	    String filePath = workspace_dir.concat(dtjmr_subtraj_dir).concat("/subtraj-").concat(Integer.toString(reducerID));
	    Path hdfsPath = new Path(filePath);

	    if (fileSystem.exists(hdfsPath)) {
	        
	    	fileOutputStream = fileSystem.append(hdfsPath);
	    	
        
	    } else {
	        
	    	fileOutputStream = fileSystem.create(hdfsPath);
	    }
        
	    writer = new PrintWriter(fileOutputStream);


	}
	
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        
    	writer.flush();
        fileOutputStream.hflush();
        writer.close();
        fileOutputStream.close();

    }

	HashMap<Integer,Period> RightTrajDuration = new HashMap<Integer,Period>();
	Period match_duration = new Period();

	DTJSubtrajPointPair output_key = new DTJSubtrajPointPair();
	ArrayList<DTJSubtrajRight> output_value = new ArrayList<DTJSubtrajRight>();
	
	PointST point = new PointST();
	
	DTJrPointElement Element = new DTJrPointElement();
	
	ArrayList<DTJrPointElement> L = new ArrayList<DTJrPointElement>();
	HashSet<Integer> F = new HashSet<Integer>();
	
	
	TreeMap<Integer,List<DTJrPointElement>> MatchList = new TreeMap<Integer,List<DTJrPointElement>>();
	HashMap<Integer,HashSet<Integer>> FalseAfter = new HashMap<Integer,HashSet<Integer>>();
	HashMap<Integer,HashSet<Integer>> FalseBefore = new HashMap<Integer,HashSet<Integer>>();
	
	HashMap<Integer,DTJrPointElement2> key_point = new HashMap<Integer,DTJrPointElement2>();

	LinkedHashMap<Integer, HashSet<DTJrPointElement>> Result = new LinkedHashMap<Integer, HashSet<DTJrPointElement>>();
	Map<Integer, List<DTJrPointElement>> tmp_result = new HashMap<Integer, List<DTJrPointElement>>();
	List<DTJrPointElement>  intersection = new ArrayList<DTJrPointElement>();
	
	LinkedHashMap<Integer,Double> voting_signal = new LinkedHashMap<Integer,Double>();
	LinkedHashMap<Integer,Integer> subtraj = new LinkedHashMap<Integer,Integer>();

	boolean match;
	boolean after;
	String match_after;
	
	int prev_obj_id = 0;
	int prev_traj_id = 0;
	PointST prev_point = new PointST();
	int prev_dt = Integer.MAX_VALUE;
	double prev_dist_sp = Integer.MAX_VALUE;;
	String line = new String();
	StringTokenizer linetokenizer;
	StringTokenizer linetokenizer2;

	

	public void reduce(DTJrPairElement _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		
		// process values
		MatchList.clear();
		FalseAfter.clear();
		FalseBefore.clear();
		Result.clear();
		voting_signal.clear();
		subtraj.clear();
		key_point.clear();
		
		double max_voting = 0;
		
		for (Text val : values) {
			
			Result.put(_key.r_p.t, new HashSet<DTJrPointElement>());
			voting_signal.put(_key.r_p.t, (double) 0);

			line = val.toString();
			
	        if (e_sp_method == 1){
	    		
		        linetokenizer = new StringTokenizer(line, ",");
		        
		        while (linetokenizer.hasMoreTokens()) {
		        	
		        	match = Boolean.parseBoolean(linetokenizer.nextToken());
		        	after = Boolean.parseBoolean(linetokenizer.nextToken());
		        	
		        }
		        	            
	    	} else if (e_sp_method == 2){
			
		        linetokenizer = new StringTokenizer(line, "|");
		        
		        
		        while (linetokenizer.hasMoreTokens()) {
		        	
		        	match_after = linetokenizer.nextToken();
		        	epsilon_sp = (int)Math.round(Double.parseDouble(linetokenizer.nextToken()));
		        	
		        }
		        
		        linetokenizer2 = new StringTokenizer(match_after, ",");

		        while (linetokenizer2.hasMoreTokens()) {
		        	
		        	match = Boolean.parseBoolean(linetokenizer2.nextToken());
		        	after = Boolean.parseBoolean(linetokenizer2.nextToken());
		        	
		        }
		        
	    	}
	        
			key_point.put(_key.r_p.t, new DTJrPointElement2(_key.r_obj_id, _key.r_traj_id, _key.r_p, epsilon_sp));


			DTJrPointElement Element = new DTJrPointElement(_key.s_obj_id, _key.s_traj_id, _key.s_p);
	        
			if (match == true){
				
				if (!MatchList.containsKey(_key.r_p.t)){
					
					if (!MatchList.isEmpty() && MatchList.lastKey() - MatchList.firstKey() >= dt + 2*epsilon_t){
						
						int l = MatchList.firstKey();
						int first_key;

						tmp_result.clear();
						tmp_result.put(l, MatchList.get(l));
						int prev_p_key = l;
						int next_l = 0;
						int counter = 0;


						for (Map.Entry<Integer,List<DTJrPointElement>> entry : MatchList.entrySet()){

							if (counter == 1){
								next_l = entry.getKey();
							}

							intersection.clear();
							
							int m = 0;
							int n = 0;

							//List intersection in O(n)
							while (m < tmp_result.get(prev_p_key).size() && n < entry.getValue().size()){

								if (tmp_result.get(prev_p_key).get(m).obj_id > entry.getValue().get(n).obj_id){
									
									n++;
									
								} else if (tmp_result.get(prev_p_key).get(m).obj_id < entry.getValue().get(n).obj_id){
									
									m++;
									
								} else if (tmp_result.get(prev_p_key).get(m).obj_id == entry.getValue().get(n).obj_id){
									
									if (tmp_result.get(prev_p_key).get(m).traj_id > entry.getValue().get(n).traj_id){
										
										n++;
										
									} else if (tmp_result.get(prev_p_key).get(m).traj_id < entry.getValue().get(n).traj_id){
										
										m++;
										
									} else if (tmp_result.get(prev_p_key).get(m).traj_id == entry.getValue().get(n).traj_id){
										
										if (entry.getValue().get(n).obj_id != 0 && entry.getValue().get(n).traj_id !=0){

											if ((!FalseAfter.isEmpty() && FalseAfter.containsKey(prev_p_key) && entry.getKey() != l  && FalseAfter.get(prev_p_key).contains(entry.getValue().get(n).obj_id)) || (!FalseBefore.isEmpty() && FalseBefore.containsKey(entry.getKey()) && entry.getKey() != l && FalseBefore.get(entry.getKey()).contains(entry.getValue().get(n).obj_id))){
												
												//Do Nothing
			
											} else {
												
												if (!RightTrajDuration.containsKey(entry.getValue().get(n).obj_id)){
												
													RightTrajDuration.put(entry.getValue().get(n).obj_id, new Period(Math.max(entry.getValue().get(n).point.t, entry.getKey()), Math.min(entry.getValue().get(n).point.t, entry.getKey())));
												
												} else {

													if (Math.max(entry.getValue().get(n).point.t, entry.getKey()) < RightTrajDuration.get(entry.getValue().get(n).obj_id).ti){

														RightTrajDuration.get(entry.getValue().get(n).obj_id).setPeriod_ti(Math.max(entry.getValue().get(n).point.t, entry.getKey()));
													
													} 
													if (Math.min(entry.getValue().get(n).point.t, entry.getKey()) > RightTrajDuration.get(entry.getValue().get(n).obj_id).te){
	
														RightTrajDuration.get(entry.getValue().get(n).obj_id).setPeriod_te(Math.min(entry.getValue().get(n).point.t, entry.getKey()));
	
														
													}
												
											}
	
												intersection.add(entry.getValue().get(n));
												
											}

										}

										m++;
										n++;
										
									}

								}

							}

							tmp_result.put(entry.getKey(), new ArrayList<DTJrPointElement>(intersection));

							prev_p_key = entry.getKey();
							counter ++;

						}
								
						first_key = l;
						
						for (Map.Entry<Integer, List<DTJrPointElement>> p : MatchList.entrySet()){

							if (p.getKey() - first_key >= dt){
								
								int counter2 = 0;
								int next_first_key = 0;
								
								for (Map.Entry<Integer, List<DTJrPointElement>> pp : MatchList.subMap(first_key, true, p.getKey(), true).entrySet()){

									if (counter2 == 1){
										next_first_key = p.getKey();
									}
									
									intersection.clear();
									
									int m = 0;
									int n = 0;
									
									while (m < tmp_result.get(p.getKey()).size() && n < tmp_result.get(pp.getKey()).size()){
										

										if (tmp_result.get(p.getKey()).get(m).obj_id > tmp_result.get(pp.getKey()).get(n).obj_id){
											
											n++;
											
										} else if (tmp_result.get(p.getKey()).get(m).obj_id < tmp_result.get(pp.getKey()).get(n).obj_id){
											
											m++;
											
										} else if (tmp_result.get(p.getKey()).get(m).obj_id == tmp_result.get(pp.getKey()).get(n).obj_id){
											
											if (tmp_result.get(p.getKey()).get(m).traj_id > tmp_result.get(pp.getKey()).get(n).traj_id){
												
												n++;
												
											} else if (tmp_result.get(p.getKey()).get(m).traj_id < tmp_result.get(pp.getKey()).get(n).traj_id){
												
												m++;
												
											} else if (tmp_result.get(p.getKey()).get(m).traj_id == tmp_result.get(pp.getKey()).get(n).traj_id){

												if (RightTrajDuration.get(tmp_result.get(pp.getKey()).get(n).obj_id).Duration() >= dt){

													intersection.add(tmp_result.get(pp.getKey()).get(n));
											
												}

												m++;
												n++;
												
											}
										}

									}
									
									if (!Result.containsKey(pp.getKey())){

										Result.put(pp.getKey(), new HashSet<DTJrPointElement>(intersection));
										
										for (int j = 0; j < intersection.size(); j++){
											
											double new_voting = 0;
									    		
											new_voting = key_point.get(pp.getKey()).point.p.SimilaritySP(intersection.get(j).point.toPointSP(), key_point.get(pp.getKey()).epsilon_sp);
											
											double new_sum = voting_signal.get(pp.getKey()) + new_voting;
											
											if (new_sum > max_voting){
												max_voting = new_sum;
											}
											
											voting_signal.put(pp.getKey(), new_sum);

										}


									} else {
										
										for (int j = 0; j < intersection.size(); j++){
											
											if (!Result.get(pp.getKey()).contains(intersection.get(j))){
												
												double new_voting = 0;
												
												new_voting = key_point.get(pp.getKey()).point.p.SimilaritySP(intersection.get(j).point.toPointSP(), key_point.get(pp.getKey()).epsilon_sp);

												double new_sum = voting_signal.get(pp.getKey()) + new_voting;
												
												if (new_sum > max_voting){
													
													max_voting = new_sum;
												
												}
												
												voting_signal.put(pp.getKey(), new_sum);
												Result.get(pp.getKey()).add(intersection.get(j));
											} 							
										}
									}
									counter2++;
								}
								
								first_key = next_first_key;
							}

						}

						l = next_l;
					

						FalseAfter.remove(MatchList.firstKey());
						FalseBefore.remove(MatchList.firstKey());
						MatchList.remove(MatchList.firstKey());
					
					}
					
					ArrayList<DTJrPointElement> L = new ArrayList<DTJrPointElement>();
					
					L.add(Element);
					MatchList.put(_key.r_p.t, L);
					prev_dt = Math.abs(Element.point.t - _key.r_p.t);
					prev_dist_sp = Element.point.p.DistanceSP(_key.r_p.p);

				} else {
					
					if (Element.obj_id == prev_obj_id && Element.traj_id == prev_traj_id){
						
						if (Element.point.p.DistanceSP(_key.r_p.p) < prev_dist_sp){

							prev_dt = Math.abs(Element.point.t - _key.r_p.t);
							prev_dist_sp = Element.point.p.DistanceSP(_key.r_p.p);

							MatchList.get(_key.r_p.t).remove(MatchList.get(_key.r_p.t).size()-1);
							MatchList.get(_key.r_p.t).add(Element);
							
						}
						
					} else {
						
						 MatchList.get(_key.r_p.t).add(Element);
						 prev_dt = Math.abs(Element.point.t - _key.r_p.t);
						 prev_dist_sp = Element.point.p.DistanceSP(_key.r_p.p);

					}
					
				}
				
				prev_obj_id = Element.obj_id;
				prev_traj_id = Element.traj_id;
				prev_point = new PointST(Element.point);

			} else{
				
				if (after == true){
					
					if (!FalseAfter.containsKey(_key.r_p.t)){
						
						HashSet<Integer> F = new HashSet<Integer>();
						
						F.add(Element.obj_id);
						FalseAfter.put(_key.r_p.t, F);

					} else {
			
						FalseAfter.get(_key.r_p.t).add(Element.obj_id);

					}
					
				} else {
					
					if (!FalseBefore.containsKey(_key.r_p.t)){
						
						HashSet<Integer> F = new HashSet<Integer>();
				
						F.add(Element.obj_id);
						FalseBefore.put(_key.r_p.t, F);

					} else {
			
						FalseBefore.get(_key.r_p.t).add(Element.obj_id);

					}
	
				}


			}
		}
		
		if (!MatchList.isEmpty()){
			
			int l = MatchList.firstKey();
			int first_key;
			
			RightTrajDuration.clear();

			tmp_result.clear();
			tmp_result.put(l, MatchList.get(l));
			int prev_p_key = l;
			int next_l = 0;
			int counter = 0;


			for (Map.Entry<Integer,List<DTJrPointElement>> entry : MatchList.entrySet()){

				if (counter == 1){
					next_l = entry.getKey();
				}

				intersection.clear();
				
				int m = 0;
				int n = 0;

				//List intersection in O(n)
				while (m < tmp_result.get(prev_p_key).size() && n < entry.getValue().size()){

					if (tmp_result.get(prev_p_key).get(m).obj_id > entry.getValue().get(n).obj_id){
						
						n++;
						
					} else if (tmp_result.get(prev_p_key).get(m).obj_id < entry.getValue().get(n).obj_id){
						
						m++;
						
					} else if (tmp_result.get(prev_p_key).get(m).obj_id == entry.getValue().get(n).obj_id){
						
						if (tmp_result.get(prev_p_key).get(m).traj_id > entry.getValue().get(n).traj_id){
							
							n++;
							
						} else if (tmp_result.get(prev_p_key).get(m).traj_id < entry.getValue().get(n).traj_id){
							
							m++;
							
						} else if (tmp_result.get(prev_p_key).get(m).traj_id == entry.getValue().get(n).traj_id){
							
							if (entry.getValue().get(n).obj_id != 0 && entry.getValue().get(n).traj_id !=0){

								if ((!FalseAfter.isEmpty() && FalseAfter.containsKey(prev_p_key) && entry.getKey() != l  && FalseAfter.get(prev_p_key).contains(entry.getValue().get(n).obj_id)) || (!FalseBefore.isEmpty() && FalseBefore.containsKey(entry.getKey()) && entry.getKey() != l && FalseBefore.get(entry.getKey()).contains(entry.getValue().get(n).obj_id))){
									
									//Do Nothing

								} else {
									
									if (!RightTrajDuration.containsKey(entry.getValue().get(n).obj_id)){
									
										RightTrajDuration.put(entry.getValue().get(n).obj_id, new Period(Math.max(entry.getValue().get(n).point.t, entry.getKey()), Math.min(entry.getValue().get(n).point.t, entry.getKey())));
									
									} else {
	
										if (Math.max(entry.getValue().get(n).point.t, entry.getKey()) < RightTrajDuration.get(entry.getValue().get(n).obj_id).ti){
	
											RightTrajDuration.get(entry.getValue().get(n).obj_id).setPeriod_ti(Math.max(entry.getValue().get(n).point.t, entry.getKey()));
											
										} 
										if (Math.min(entry.getValue().get(n).point.t, entry.getKey()) > RightTrajDuration.get(entry.getValue().get(n).obj_id).te){
	
											RightTrajDuration.get(entry.getValue().get(n).obj_id).setPeriod_te(Math.min(entry.getValue().get(n).point.t, entry.getKey()));
	
											
										}
										
									}

									intersection.add(entry.getValue().get(n));
									
								}

							}

							m++;
							n++;
							
						}

					}

				}

				tmp_result.put(entry.getKey(), new ArrayList<DTJrPointElement>(intersection));

				prev_p_key = entry.getKey();
				counter ++;

			}
					
			first_key = l;
			
			for (Map.Entry<Integer, List<DTJrPointElement>> p : MatchList.entrySet()){

				if (p.getKey() - first_key >= dt){
					
					int counter2 = 0;
					int next_first_key = 0;
					
					for (Map.Entry<Integer, List<DTJrPointElement>> pp : MatchList.subMap(first_key, true, p.getKey(), true).entrySet()){

						if (counter2 == 1){
							next_first_key = p.getKey();
						}
						
						intersection.clear();
						
						int m = 0;
						int n = 0;
						
						while (m < tmp_result.get(p.getKey()).size() && n < tmp_result.get(pp.getKey()).size()){
							

							if (tmp_result.get(p.getKey()).get(m).obj_id > tmp_result.get(pp.getKey()).get(n).obj_id){
								
								n++;
								
							} else if (tmp_result.get(p.getKey()).get(m).obj_id < tmp_result.get(pp.getKey()).get(n).obj_id){
								
								m++;
								
							} else if (tmp_result.get(p.getKey()).get(m).obj_id == tmp_result.get(pp.getKey()).get(n).obj_id){
								
								if (tmp_result.get(p.getKey()).get(m).traj_id > tmp_result.get(pp.getKey()).get(n).traj_id){
									
									n++;
									
								} else if (tmp_result.get(p.getKey()).get(m).traj_id < tmp_result.get(pp.getKey()).get(n).traj_id){
									
									m++;
									
								} else if (tmp_result.get(p.getKey()).get(m).traj_id == tmp_result.get(pp.getKey()).get(n).traj_id){
	
									if (RightTrajDuration.get(tmp_result.get(pp.getKey()).get(n).obj_id).Duration() >= dt){

										intersection.add(tmp_result.get(pp.getKey()).get(n));
								
									}
									m++;
									n++;
									
								}
							}

						}
						
						if (!Result.containsKey(pp.getKey())){

							Result.put(pp.getKey(), new HashSet<DTJrPointElement>(intersection));
							
							for (int j = 0; j < intersection.size(); j++){
								
								double new_voting = 0;
								
						    		
								new_voting = key_point.get(pp.getKey()).point.p.SimilaritySP(intersection.get(j).point.toPointSP(), key_point.get(pp.getKey()).epsilon_sp);

								double new_sum = voting_signal.get(pp.getKey()) + new_voting;
								
								if (new_sum > max_voting){
									
									max_voting = new_sum;
								}
								
								voting_signal.put(pp.getKey(), new_sum);

							}
							
						} else {
							
							for (int j = 0; j < intersection.size(); j++){
								
								if (!Result.get(pp.getKey()).contains(intersection.get(j))){
									
									double new_voting = 0;
									
									new_voting = key_point.get(pp.getKey()).point.p.SimilaritySP(intersection.get(j).point.toPointSP(), key_point.get(pp.getKey()).epsilon_sp);
	
									double new_sum = voting_signal.get(pp.getKey()) + new_voting;
									
									if (new_sum > max_voting){
										
										max_voting = new_sum;
										
									}
									
									voting_signal.put(pp.getKey(), new_sum);
									Result.get(pp.getKey()).add(intersection.get(j));
								} 							
							}
						}
						
						counter2++;
					}
					
					first_key = next_first_key;
				}

			}

			l = next_l;
		
			FalseAfter.remove(MatchList.firstKey());
			FalseBefore.remove(MatchList.firstKey());
			MatchList.remove(MatchList.firstKey());

		}

	//Segmentation1
	if (SegmentationAlgorithm == 1){
		
			int subtraj_id = 1;
			int counter_vs = 1;
			ArrayList<Double> w1 = new ArrayList<Double>(w);
			ArrayList<Double> w2 = new ArrayList<Double>(w);
			ArrayList<Double> d_w1 = new ArrayList<Double>(w);
			ArrayList<Double> d_w2 = new ArrayList<Double>(w);
			ArrayList<Integer> next_n = new ArrayList<Integer>(w);
			
			boolean breaktraj = false;

			
			for (Map.Entry<Integer,Double> entry_sv : voting_signal.entrySet()){
				
				subtraj.put(entry_sv.getKey(), subtraj_id);
					
				if (max_voting > 0){
					double normalized_voting = voting_signal.get(entry_sv.getKey())/max_voting;
					voting_signal.put(entry_sv.getKey(), normalized_voting);
				}
				
				if (counter_vs <= w){
					
					w1.add(voting_signal.get(entry_sv.getKey()));
					d_w1.add((double)0);
					
				} else if (counter_vs < 2*w){
					
					w2.add(voting_signal.get(entry_sv.getKey()));
					d_w2.add((double)0);
					next_n.add(entry_sv.getKey());
					
				} else if (counter_vs == 2*w){
					
					w2.add(voting_signal.get(entry_sv.getKey()));
					next_n.add(entry_sv.getKey());

					double sum_w1 = (double)0;
					double sum_w2 = (double)0;
					double mean_w1 = (double)0;
					double mean_w2 = (double)0;
					double d = (double)0;

					for (int i = 0; i < w; i++){
					
						sum_w1 = sum_w1 + w1.get(i);
						sum_w2 = sum_w2 + w2.get(i);
					
					}
					
					mean_w1 = (double)sum_w1/w;
					mean_w2 = (double)sum_w2/w;

					d = Math.abs(mean_w1 - mean_w2);
					d_w2.add(d);

					if(d > tau && d >= Math.max(Collections.max(d_w1), Collections.max(d_w2))){

						subtraj_id++;

						w1.clear();
						w1.addAll(w2);
						w2.clear();
						
						d_w1.clear();
						d_w1.addAll(d_w2);
						d_w2.clear();
						
						breaktraj = true;

						counter_vs = w;
					}
					
				} else {
					
					w1.remove(0);
					w1.add(w2.get(0));
					
					w2.remove(0);
					w2.add(voting_signal.get(entry_sv.getKey()));
					
					d_w1.remove(0);
					d_w1.add(d_w2.get(0));
					
					d_w2.remove(0);

					next_n.remove(0);
					next_n.add(entry_sv.getKey());

					
					double sum_w1 = (double)0;
					double sum_w2 = (double)0;
					double mean_w1 = (double)0;
					double mean_w2 = (double)0;
					double d = (double)0;

					for (int i = 0; i < w; i++){
					
						sum_w1 = sum_w1 + w1.get(i);
						sum_w2 = sum_w2 + w2.get(i);
					
					}
					
					mean_w1 = (double)sum_w1/w;
					mean_w2 = (double)sum_w2/w;
					
					d = Math.abs(mean_w1 - mean_w2);
					d_w2.add(d);

					if(d > tau && d >= Math.max(Collections.max(d_w1), Collections.max(d_w2))){
						
						subtraj_id++;
						
						w1.clear();
						w1.addAll(w2);
						w2.clear();
						
						d_w1.clear();
						d_w1.addAll(d_w2);
						d_w2.clear();
						
						breaktraj = true;

						counter_vs = w;

					}

				}
				
				if (!breaktraj){
					
					subtraj.put(entry_sv.getKey(), subtraj_id);

				} else {

					for(int i = 0; i < next_n.size(); i++){
						
						subtraj.put(next_n.get(i), subtraj_id);

					}
					next_n.clear();
					breaktraj = false;
				}

				counter_vs++;
				
			}
			
		} else if (SegmentationAlgorithm == 2){ //Segmentation2
			
			int subtraj_id = 1;
			int counter_vs = 1;
			ArrayList<Integer> next_n = new ArrayList<Integer>(w);
			
			boolean breaktraj = false;

			ArrayList<HashSet<Integer>> w_1 = new ArrayList<HashSet<Integer>>();
			HashSet<Integer> w_1_tmp = new HashSet<Integer>();
			HashSet<Integer> w_1_set = new HashSet<Integer>();


			ArrayList<HashSet<Integer>> w_2 = new ArrayList<HashSet<Integer>>();
			HashSet<Integer> w_2_tmp = new HashSet<Integer>();
			HashSet<Integer> w_2_set = new HashSet<Integer>();

			HashSet<Integer> intersection = new HashSet<Integer>();
			double set_sim = (double)0;
			next_n.clear();
			

			for (Map.Entry<Integer,Double> entry_hr : voting_signal.entrySet()){
				
				subtraj.put(entry_hr.getKey(), subtraj_id);
				
				if (counter_vs <= w){
					
					w_1_tmp.clear();
					
					for(DTJrPointElement p_elem : Result.get(entry_hr.getKey())){
						
						w_1_tmp.add(p_elem.obj_id);

					}
					
					w_1.add(new HashSet<Integer>(w_1_tmp));

				} else if (counter_vs < 2*w){
					
					w_2_tmp.clear();

					for(DTJrPointElement p_elem : Result.get(entry_hr.getKey())){
						
						w_2_tmp.add(p_elem.obj_id);

					}
					
					w_2.add(new HashSet<Integer>(w_2_tmp));

					next_n.add(entry_hr.getKey());

					
				} else if (counter_vs == 2*w){
					
					w_2_tmp.clear();

					for (DTJrPointElement p_elem : Result.get(entry_hr.getKey())){
						
						w_2_tmp.add(p_elem.obj_id);
					}
					
					w_2.add(new HashSet<Integer>(w_2_tmp));

					next_n.add(entry_hr.getKey());

					intersection.clear();
					
					w_1_set.clear();
					w_2_set.clear();

					for (int i = 0; i < w; i++){
						
						w_1_set.addAll(w_1.get(i));
						w_2_set.addAll(w_2.get(i));

					}

					intersection.addAll(w_1_set);
					intersection.retainAll(w_2_set);

					if (w_1_set.size() == 0 && w_2_set.size() == 0){
						
						set_sim = 1;
						
					} else {
						
						set_sim = (double)(intersection.size())/(double)(w_1_set.size() + w_2_set.size() - intersection.size());
						
					}
					
					if (set_sim < tau){

						subtraj_id++;

						w_1.clear();
						w_1.addAll(w_2);
						w_2.clear();
						
						breaktraj = true;
						counter_vs = w;
					}
					
				} else {
					
					w_1.remove(0);
					
					w_1.add(new HashSet<Integer>(w_2.get(0)));
					
					w_2.remove(0);

					w_2_tmp.clear();
					
					for(DTJrPointElement p_elem : Result.get(entry_hr.getKey())){
						w_2_tmp.add(p_elem.obj_id);
					}
					
					w_2.add(new HashSet<Integer>(w_2_tmp));

					next_n.remove(0);
					next_n.add(entry_hr.getKey());

					intersection.clear();
					
					w_1_set.clear();
					w_2_set.clear();

					for (int i = 0; i < w; i++){
						
						w_1_set.addAll(w_1.get(i));
						w_2_set.addAll(w_2.get(i));

					}

					intersection.addAll(w_1_set);
					intersection.retainAll(w_2_set);
					
					if(w_1_set.size() == 0 && w_2_set.size() == 0){
						set_sim = 1;
					} else {
						set_sim = (double)(intersection.size())/(double)(w_1_set.size() + w_2_set.size() - intersection.size());
					}
					
					if(set_sim < tau){

						subtraj_id++;

						w_1.clear();
						w_1.addAll(w_2);
						w_2.clear();
						
						
						breaktraj = true;
						counter_vs = w;
					}

				}
				
				if (!breaktraj){
					
					subtraj.put(entry_hr.getKey(), subtraj_id);


				} else {

					for(int i = 0; i < next_n.size(); i++){
						
						subtraj.put(next_n.get(i), subtraj_id);

					}
					
					next_n.clear();
					breaktraj = false;
				}

				counter_vs++;

			}
			
		}

		int prev_subtra_id = 1;
		int subtraj_mint = Integer.MAX_VALUE;
		int subtraj_maxt = Integer.MIN_VALUE;
		HashMap<DTJSubtrajPointPair, ArrayList<DTJSubtrajRight>> subtraj_similarity = new HashMap<DTJSubtrajPointPair, ArrayList<DTJSubtrajRight>>();
		double sum_voting = (double)0;
		int n_of_points = 0;
		
		DTJSubtraj subtraj_key = new DTJSubtraj();
		DTJSubtrajValueShort subtraj_value = new DTJSubtrajValueShort();

		HashMap<DTJSubtraj, DTJSubtrajValueShort> subtraj_result = new HashMap<DTJSubtraj, DTJSubtrajValueShort>();
		
		for (Map.Entry<Integer, Double> entry_hr : voting_signal.entrySet()){

			if (subtraj.get(entry_hr.getKey()) == prev_subtra_id){
				
				if (subtraj_mint > entry_hr.getKey()){
					
					subtraj_mint = entry_hr.getKey();
					
				}
				
				if (subtraj_maxt < entry_hr.getKey()){
					
					subtraj_maxt = entry_hr.getKey();
					
				}
				
				if (Result.containsKey(entry_hr.getKey())){
					
					for (DTJrPointElement p_elem : Result.get(entry_hr.getKey())){
						
						DTJSubtrajPointPair left = new DTJSubtrajPointPair(_key.r_obj_id, _key.r_traj_id, subtraj.get(entry_hr.getKey()), p_elem.obj_id, p_elem.traj_id);
						
						double sim = 0;
				    		
						sim = key_point.get(entry_hr.getKey()).point.p.SimilaritySP(p_elem.point.toPointSP(), key_point.get(entry_hr.getKey()).epsilon_sp);
				        
						DTJSubtrajRight right = new DTJSubtrajRight(entry_hr.getKey(), p_elem.point.t, sim);
						if (!subtraj_similarity.containsKey(left)){
							ArrayList<DTJSubtrajRight> r_part = new ArrayList<DTJSubtrajRight>();
							r_part.add(right);
							subtraj_similarity.put(left, r_part);
						} else {
							subtraj_similarity.get(left).add(right);
						}
						
						sum_voting = sum_voting + sim;
					}
					
				}
				
				n_of_points++;
				
			} else {
				
				subtraj_key = new DTJSubtraj(_key.r_obj_id, _key.r_traj_id, prev_subtra_id);
				subtraj_value = new DTJSubtrajValueShort(subtraj_mint, subtraj_maxt, n_of_points, sum_voting);

				subtraj_result.put(subtraj_key, subtraj_value);
				
			    subtraj_mint = Integer.MAX_VALUE;
				subtraj_maxt = Integer.MIN_VALUE;
				n_of_points = 0;
				sum_voting = 0;
				
				if (subtraj_mint > entry_hr.getKey()){
					
					subtraj_mint = entry_hr.getKey();
					
				}
				
				if (subtraj_maxt < entry_hr.getKey()){
					
					subtraj_maxt = entry_hr.getKey();
					
				}

				
				if (Result.containsKey(entry_hr.getKey())){
					
					for (DTJrPointElement p_elem : Result.get(entry_hr.getKey())){
						
						DTJSubtrajPointPair left = new DTJSubtrajPointPair(_key.r_obj_id, _key.r_traj_id, subtraj.get(entry_hr.getKey()), p_elem.obj_id, p_elem.traj_id);
						
						double sim = 0;

						sim = key_point.get(entry_hr.getKey()).point.p.SimilaritySP(p_elem.point.toPointSP(), key_point.get(entry_hr.getKey()).epsilon_sp);

						DTJSubtrajRight right = new DTJSubtrajRight(entry_hr.getKey(), p_elem.point.t, sim);

						if (!subtraj_similarity.containsKey(left)){
							ArrayList<DTJSubtrajRight> r_part = new ArrayList<DTJSubtrajRight>();
							r_part.add(right);
							subtraj_similarity.put(left, r_part);
						} else {
							subtraj_similarity.get(left).add(right);
						}
						
						sum_voting = sum_voting + sim;
					}
						
				}

				n_of_points++;
		    }
				
			prev_subtra_id = subtraj.get(entry_hr.getKey());
		}
		
		subtraj_key = new DTJSubtraj(_key.r_obj_id, _key.r_traj_id, prev_subtra_id);
		subtraj_value = new DTJSubtrajValueShort(subtraj_mint, subtraj_maxt, n_of_points, sum_voting);

		subtraj_result.put(subtraj_key, subtraj_value);

		for (Map.Entry<DTJSubtrajPointPair, ArrayList<DTJSubtrajRight>> entry_ss : subtraj_similarity.entrySet()){
			
			output_key = new DTJSubtrajPointPair(entry_ss.getKey());
			output_value = new ArrayList<DTJSubtrajRight>(entry_ss.getValue());
			
			context.write(output_key, output_value);

		}
		
        
		for (Map.Entry<DTJSubtraj, DTJSubtrajValueShort> entry_sr : subtraj_result.entrySet()){
            writer.append(entry_sr.getKey().obj_id + "," + entry_sr.getKey().traj_id + "," + entry_sr.getKey().subtraj_id + "," + entry_sr.getValue().mint + "," + entry_sr.getValue().maxt + "," + entry_sr.getValue().n_of_points + "," + entry_sr.getValue().sum_voting + "\n");
        }
		

	}

}
