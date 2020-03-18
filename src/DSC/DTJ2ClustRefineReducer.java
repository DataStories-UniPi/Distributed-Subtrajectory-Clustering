package DSC;


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.mapreduce.Reducer;
import CustomWritables.DTJSubtraj;
import CustomWritables.DTJSubtrajIntersectPart;
import CustomWritables.DTJSubtrajPairSim;
import CustomWritables.DTJSubtrajSim;

public class DTJ2ClustRefineReducer extends Reducer<DTJSubtrajIntersectPart, DTJSubtrajPairSim, DTJSubtraj, DTJSubtrajSim> {
	
	DTJSubtrajIntersectPart prev_key = new DTJSubtrajIntersectPart();
	DTJSubtrajSim prev_value = new DTJSubtrajSim();
	
	DTJSubtrajSim repr = new DTJSubtrajSim();
	DTJSubtraj cl_member = new DTJSubtraj();
	
	DTJSubtraj curr_subtraj = new DTJSubtraj();
	String curr_flag = new String();
	String prev_flag = new String();

	int partition_t;
	
	HashMap<DTJSubtraj, DTJSubtrajSim> part_prev = new HashMap<DTJSubtraj, DTJSubtrajSim>();
	HashMap<DTJSubtraj, DTJSubtrajSim> part_curr = new HashMap<DTJSubtraj, DTJSubtrajSim>();
	HashSet<DTJSubtraj> intersection = new HashSet<DTJSubtraj>();

	DTJSubtraj output_key = new DTJSubtraj();
	DTJSubtrajSim output_value = new DTJSubtrajSim();

	public void reduce(DTJSubtrajIntersectPart _key, Iterable<DTJSubtrajPairSim> values, Context context)
			throws IOException, InterruptedException {
		
		part_curr.clear();
		part_prev.clear();
		
		int prev_t = -1
				;
		for (DTJSubtrajPairSim val : values) {
			

			repr = new DTJSubtrajSim(val.r_obj_id, val.r_traj_id, val.r_subtraj_id, val.sim);
			cl_member = new DTJSubtraj(val.s_obj_id, val.s_traj_id, val.s_subtraj_id);
			partition_t = _key.t_min;

			if (repr.equals(new DTJSubtraj(-2,-2,-2))){
			
				if (partition_t == prev_t){
					
					//add subtraj --> part_curr
					part_curr.put(cl_member, repr);
					
				} else {

					if (!part_curr.isEmpty()){
						
						if (!part_prev.isEmpty()){
							
							//intersect part_prev with part_curr
							intersection.addAll(part_curr.keySet());
							intersection.retainAll(part_prev.keySet());
							
							Iterator<DTJSubtraj> iter;
							iter = intersection.iterator();
							
							while(iter.hasNext()){
								
								curr_subtraj = iter.next();
								
								//part_prev
								if (curr_subtraj.equals(part_prev.get(curr_subtraj))){//if Repr 
									
									prev_flag = "R";
									
								} else if (part_prev.get(curr_subtraj).equals(new DTJSubtraj(-1,-1,-1))){//if outlier
									
									prev_flag = "O";

								} else {//if cluster member
									
									prev_flag = "C";

								}
								
								//part_curr
								if (curr_subtraj.equals(part_curr.get(curr_subtraj))){//if Repr 
									
									curr_flag = "R";
									
								} else if (part_curr.get(curr_subtraj).equals(new DTJSubtraj(-1,-1,-1))){//if outlier
									
									curr_flag = "O";

								} else {//if cluster member
									
									curr_flag = "C";

								}

								if(curr_flag.equals("O") && prev_flag.equals("O")){//case (a):O-O

									part_prev.remove(curr_subtraj);
									
								} else
								
								if (curr_flag.equals("R") && prev_flag.equals("R")){//case (b):R-R
									
									part_prev.remove(curr_subtraj);


								} else
								
								if (curr_flag.equals("C") && prev_flag.equals("C")){//case (c):C-C
									

									if (part_curr.get(curr_subtraj).sim > part_prev.get(curr_subtraj).sim){

										part_prev.remove(curr_subtraj);
									
									} else {
									
										part_curr.remove(curr_subtraj);
										part_curr.put(curr_subtraj, part_prev.get(curr_subtraj));
										part_prev.remove(curr_subtraj);

									}
								
								} else
								
								if ((curr_flag.equals("R") && !prev_flag.equals("R")) || (!curr_flag.equals("R") && prev_flag.equals("R"))){//case (d),(e):R-C or R-O
									

									if (curr_flag.equals("R")){

										part_prev.remove(curr_subtraj);
										
									} else {
										
										part_curr.remove(curr_subtraj);
										part_curr.put(curr_subtraj, part_prev.get(curr_subtraj));
										part_prev.remove(curr_subtraj);

									}
									

								} else
								
								if ((curr_flag.equals("C") && prev_flag.equals("O")) || (curr_flag.equals("O") && prev_flag.equals("C"))){//case (f):C-O
									

									if (curr_flag.equals("C")){
										
										part_prev.remove(curr_subtraj);

									} else {
										
										part_curr.remove(curr_subtraj);
										part_curr.put(curr_subtraj, part_prev.get(curr_subtraj));
										part_prev.remove(curr_subtraj);

									}
									
								}

							}
							


							//emit part_prev
							for (Map.Entry<DTJSubtraj, DTJSubtrajSim> entry_prev : part_prev.entrySet()){
								
								output_key = new DTJSubtraj(entry_prev.getValue().toDTJSubtraj());
								output_value = new DTJSubtrajSim(entry_prev.getKey().obj_id, entry_prev.getKey().traj_id, entry_prev.getKey().subtraj_id, entry_prev.getValue().sim);
								
								context.write(output_key, output_value);
							
							}
							
							//intersetcion.clear();
							intersection.clear();
							
							//truncate part_prev
							part_prev.clear();
		
						}
						
						//move part_curr --> part_prev
						part_prev.putAll(part_curr);
						//truncate part_curr
						part_curr.clear();
						part_curr.put(cl_member, repr);


					} else {
						
						//add subtraj --> part_curr
						part_curr.put(cl_member, repr);

						
					}

				}
				
				prev_t = partition_t;

				
			} else {
				
				output_key = new DTJSubtraj(repr.toDTJSubtraj());
				output_value = new DTJSubtrajSim(val.s_obj_id, val.s_traj_id, val.s_subtraj_id, val.sim);
				
				context.write(output_key, output_value);
				
			}

		}
		
		//Emit last part
		for (Map.Entry<DTJSubtraj, DTJSubtrajSim> entry_prev : part_prev.entrySet()){
			
			output_key = new DTJSubtraj(entry_prev.getValue().toDTJSubtraj());
			output_value = new DTJSubtrajSim(entry_prev.getKey().obj_id, entry_prev.getKey().traj_id, entry_prev.getKey().subtraj_id, entry_prev.getValue().sim);
			
			context.write(output_key, output_value);
		
		}

	
	}

}
