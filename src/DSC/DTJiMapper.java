package DSC;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import CustomWritables.DTJiPointElement2;
import CustomWritables.DTJobjPair;
import CustomWritables.DTJrPairElement;
import DataTypes.BoxSP;
import DataTypes.OctTreeNode;
import DataTypes.PointSP;
import DataTypes.PointST;

public class DTJiMapper extends Mapper<LongWritable, Text, DTJrPairElement, Text> {
	
    int e_sp_method;

	int epsilon_sp;
    double epsilon_sp_prcnt;
    int epsilon_t;
    int dt;

	Text output_value = new Text();
	StringBuilder sbldr = new StringBuilder();
	int n_of_args;
	boolean match;
	boolean after;
	
	ArrayList<DTJiPointElement2> R = new ArrayList<DTJiPointElement2>();

	DTJrPairElement Element = new DTJrPairElement();
	HashMap<Integer, ArrayList<Integer>> TrI = new HashMap<Integer, ArrayList<Integer>>();
	HashMap<Integer, ArrayList<Integer>> SpI = new HashMap<Integer, ArrayList<Integer>>();
	HashMap<Integer, OctTreeNode> Mesh = new HashMap<Integer, OctTreeNode>();

	DTJobjPair objPair = new DTJobjPair();
	DTJobjPair posPair = new DTJobjPair();
	HashMap<DTJobjPair,DTJobjPair> LastPairs = new HashMap<DTJobjPair, DTJobjPair>();

	HashSet<DTJiPointElement2> NoMatch = new HashSet<DTJiPointElement2>();

	int cellID;
	int cell_lx;
	int cell_ly;
	int cell_hx;
	int cell_hy;
 	BoxSP cell = new BoxSP();
 	OctTreeNode qNode = new OctTreeNode();

 	ArrayList<Integer> CandidateNodes = new ArrayList<Integer>();
 	ArrayList<Integer> CandidateNodesTemp = new ArrayList<Integer>();
 	ArrayList<Integer> CandidateNodesFinal = new ArrayList<Integer>();

 	int q1;
 	int q2;
 	int q3;
 	int q4;
 	int q5;
 	int q6;
 	int q7;
 	int q8;

	
 	DTJrPairElement out_key = new DTJrPairElement();
 	DTJrPairElement out_value = new DTJrPairElement();
 	DTJiPointElement2 r = new DTJiPointElement2();

	String line = new String();
	StringTokenizer linetokenizer = new StringTokenizer(line, ",");
    int obj_id = 0; 
    int traj_id = 0;
    
    String flight = new String();
    
    PointST point = new PointST();
    DTJiPointElement2 pElement = new DTJiPointElement2();
	
	String[] trim;
	int start;
	int end;
	boolean pair_found = false;
	boolean pair_found2 = false;
	int i;
	int ii;
	int j;
	int jj;
	
	int ii_SpI_Pos;
	int j_TrI_pos;
	int jj_TrI_pos;
	int jj_SpI;
	
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		
		line = ivalue.toString();
        linetokenizer = new StringTokenizer(line, ",");
		n_of_args = linetokenizer.countTokens();
		
		if (n_of_args == 5){
			while (linetokenizer.hasMoreTokens()) {
				obj_id = Integer.parseInt(linetokenizer.nextToken());
				traj_id = Integer.parseInt(linetokenizer.nextToken());
				point = new PointST(Integer.parseInt(linetokenizer.nextToken()), new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken())));
			}
			
		} else if (n_of_args == 6) {
			while (linetokenizer.hasMoreTokens()) {
				obj_id = Integer.parseInt(linetokenizer.nextToken());
				traj_id = Integer.parseInt(linetokenizer.nextToken());
				point = new PointST(Integer.parseInt(linetokenizer.nextToken()), new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken())));
			}
		}
		
        if (point.t >= start - epsilon_t  && point.t <= end + epsilon_t){

	    	pElement = new DTJiPointElement2(obj_id, traj_id, point);
	    	
	    	CandidateNodes = new ArrayList<Integer>();
	    	CandidateNodes.add(Mesh.get(0).q1ID);
	    	CandidateNodes.add(Mesh.get(0).q2ID);
	    	CandidateNodes.add(Mesh.get(0).q3ID);
	    	CandidateNodes.add(Mesh.get(0).q4ID);
	    	if (n_of_args == 6){
		    	CandidateNodes.add(Mesh.get(0).q5ID);
		    	CandidateNodes.add(Mesh.get(0).q6ID);
		    	CandidateNodes.add(Mesh.get(0).q7ID);
		    	CandidateNodes.add(Mesh.get(0).q8ID);
	    	}
	    	
	    	CandidateNodesFinal = new ArrayList<Integer>();

	    	while (CandidateNodes.size() > 0){
	
		    	CandidateNodesTemp = new ArrayList<Integer>();
		    	
		    	for (int cn = 0; cn < CandidateNodes.size(); cn++){
			    	
		    		cellID = CandidateNodes.get(cn);

			    	cell = new BoxSP(Mesh.get(cellID).cell);
			    	
			    	if (e_sp_method == 1){
			    		
			    		epsilon_sp = (int)Math.round(epsilon_sp_prcnt);
			    	
			    	} else if (e_sp_method == 2){
			    		
				    	epsilon_sp = (int)Math.floor(Mesh.get(r.cellID).cell.BoxSPDiameter() * epsilon_sp_prcnt);
			    		
			    	}
			    	
			    	
			    	if (cell.BoxSPExpand(epsilon_sp).BoxSPContains(point.toPointSP())){
			    		if (n_of_args == 5){
				    		if (Mesh.get(CandidateNodes.get(cn)).q1ID ==0 && Mesh.get(CandidateNodes.get(cn)).q2ID ==0 && Mesh.get(CandidateNodes.get(cn)).q3ID ==0 && Mesh.get(CandidateNodes.get(cn)).q4ID ==0){
				    			
				    			CandidateNodesFinal.add(CandidateNodes.get(cn));

				    		} else {
				    		
				    			CandidateNodesTemp.add(Mesh.get(CandidateNodes.get(cn)).q1ID);
					    		CandidateNodesTemp.add(Mesh.get(CandidateNodes.get(cn)).q2ID);
					    		CandidateNodesTemp.add(Mesh.get(CandidateNodes.get(cn)).q3ID);
					    		CandidateNodesTemp.add(Mesh.get(CandidateNodes.get(cn)).q4ID);
					    		
				    		}
			    		} else if (n_of_args == 6) {
				    		if (Mesh.get(CandidateNodes.get(cn)).q1ID ==0 && Mesh.get(CandidateNodes.get(cn)).q2ID ==0 && Mesh.get(CandidateNodes.get(cn)).q3ID == 0 && Mesh.get(CandidateNodes.get(cn)).q4ID == 0 && Mesh.get(CandidateNodes.get(cn)).q5ID == 0 && Mesh.get(CandidateNodes.get(cn)).q6ID == 0 && Mesh.get(CandidateNodes.get(cn)).q7ID == 0 && Mesh.get(CandidateNodes.get(cn)).q8ID == 0){
				    			
				    			CandidateNodesFinal.add(CandidateNodes.get(cn));

				    		} else {
				    		
				    			CandidateNodesTemp.add(Mesh.get(CandidateNodes.get(cn)).q1ID);
					    		CandidateNodesTemp.add(Mesh.get(CandidateNodes.get(cn)).q2ID);
					    		CandidateNodesTemp.add(Mesh.get(CandidateNodes.get(cn)).q3ID);
					    		CandidateNodesTemp.add(Mesh.get(CandidateNodes.get(cn)).q4ID);
				    			CandidateNodesTemp.add(Mesh.get(CandidateNodes.get(cn)).q5ID);
					    		CandidateNodesTemp.add(Mesh.get(CandidateNodes.get(cn)).q6ID);
					    		CandidateNodesTemp.add(Mesh.get(CandidateNodes.get(cn)).q7ID);
					    		CandidateNodesTemp.add(Mesh.get(CandidateNodes.get(cn)).q8ID);
			    		
				    		}
		
			    		}

			    	}


		    	}
		    	
		    	CandidateNodes = new ArrayList<Integer>();
		    	CandidateNodes.addAll(CandidateNodesTemp);
			    	
	    		
	    	}
    		
		    for (int cn = 0; cn < CandidateNodesFinal.size(); cn++) {
		    	
		    	cellID = CandidateNodesFinal.get(cn);
		    	cell = new BoxSP(Mesh.get(cellID).cell);

    			if (cell.BoxSPContains(point.toPointSP())){
    		    	
    				pElement.setcellID(cellID);

	 			}

	 			if (!SpI.containsKey(cellID)){
					
	 				
	 				ArrayList<Integer> SpList = new ArrayList<Integer>();
					SpList.add(R.size());
					SpI.put(cellID, SpList);
									
				} else {
					
					SpI.get(cellID).add(R.size());

				}	

	    		
	    	}
	    	
		    
			R.add(pElement);
			

			if (!TrI.containsKey(obj_id)){
				
				ArrayList<Integer> HList = new ArrayList<Integer>();
				HList.add(R.size()-1);
				TrI.put(obj_id, HList);
								
			} else {
				
				TrI.get(obj_id).add(R.size()-1);
			}

        }


	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {

		setup(context);
		
        Configuration conf = context.getConfiguration();
        
        e_sp_method = Integer.parseInt(conf.get("e_sp_method"));

        epsilon_sp_prcnt = Double.parseDouble(conf.get("epsilon_sp_prcnt"));

        epsilon_t = Integer.parseInt(conf.get("epsilon_t"));

        URI[] cacheFile = context.getCacheFiles();
       	Path path = new Path(cacheFile[0]);
       	SequenceFile.Reader reader = null;
        reader = new SequenceFile.Reader(conf, Reader.file(path), Reader.bufferSize(4096), Reader.start(0));
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
     	
       
        while (reader.next(key, value)) {
        	
    		line = key.toString();
            linetokenizer = new StringTokenizer(line, ",");
    		n_of_args = linetokenizer.countTokens();
    		
    		if (n_of_args == 9){
    			while (linetokenizer.hasMoreTokens()) {
                	cellID = Integer.parseInt(linetokenizer.nextToken());
                	cell = new BoxSP(new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken())), new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()))); 
                	q1 = Integer.parseInt(linetokenizer.nextToken());
                	q2 = Integer.parseInt(linetokenizer.nextToken());
                	q3 = Integer.parseInt(linetokenizer.nextToken());
                	q4 = Integer.parseInt(linetokenizer.nextToken());    			
                }
            	qNode = new OctTreeNode(cellID, cell, q1, q2, q3, q4);

    		} else if (n_of_args == 15) {
    			while (linetokenizer.hasMoreTokens()) {
                	cellID = Integer.parseInt(linetokenizer.nextToken());
                	cell = new BoxSP(new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken())), new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()))); 
                	q1 = Integer.parseInt(linetokenizer.nextToken());
                	q2 = Integer.parseInt(linetokenizer.nextToken());
                	q3 = Integer.parseInt(linetokenizer.nextToken());
                	q4 = Integer.parseInt(linetokenizer.nextToken());
                	q5 = Integer.parseInt(linetokenizer.nextToken());
                	q6 = Integer.parseInt(linetokenizer.nextToken());
                	q7 = Integer.parseInt(linetokenizer.nextToken());
                	q8 = Integer.parseInt(linetokenizer.nextToken());  
                }
            	qNode = new OctTreeNode(cellID, cell, q1, q2, q3, q4, q5, q6, q7, q8);

    		}


        	Mesh.put(qNode.ID, qNode);

 		}

        IOUtils.closeStream(reader);
        
		trim = context.getInputSplit().toString().split(",");
		start = Integer.parseInt(trim[0].split(":")[1]);
		end = Integer.parseInt(trim[1].split(":")[1]);
	    
		if (end == Integer.MAX_VALUE){
    		end = end - epsilon_t;
    	}

		i = 0;
		
		while (context.nextKeyValue()) {
			
			map(context.getCurrentKey(), context.getCurrentValue(), context);
			

	        if (point.t >= start - epsilon_t && point.t <= end + epsilon_t){
			
		    	r = R.get(i); 
				pair_found = false;
				
		    	if (e_sp_method == 1){
		    		
		    		epsilon_sp = (int)Math.round(epsilon_sp_prcnt);
		    	
		    	} else if (e_sp_method == 2){
	    		
					epsilon_sp = (int)Math.floor(Mesh.get(r.cellID).cell.BoxSPDiameter() * epsilon_sp_prcnt);
		    		
		    	}
				
		    	ii_SpI_Pos = Collections.binarySearch(SpI.get(r.cellID), i);
				

				if (ii_SpI_Pos < SpI.get(r.cellID).size()){

	  			  ii = SpI.get(r.cellID).get(ii_SpI_Pos);
	  			  
	    		  while (ii_SpI_Pos >= 0 && (R.get(ii).getPoint().t >= r.point.t - epsilon_t)){

	    			  if ((r.point.toPointSP().DistanceSP(R.get(ii).point.toPointSP()) <= epsilon_sp) && (r.obj_id != R.get(ii).obj_id)) {
							
	    				  pair_found = true;
		    			
			    		  match = true;
			    		  after = true;
			    		  
			    		  sbldr.setLength(0);
						
			    		  if (e_sp_method == 1){
							
				    		  sbldr.append(match).append(",").append(after);
						
			    		  } else if (e_sp_method == 2){
						
				    		  sbldr.append(match).append(",").append(after).append("|").append(Mesh.get(r.cellID).cell.BoxSPDiameter());
							
			    		  }
					      
			    		  output_value.set(sbldr.toString());
			    		  
			    		  
	    				  Element.setr_obj_id(r.obj_id);
	    				  Element.setr_traj_id(r.traj_id);
	    				  Element.setr_point(r.point);
		    			  Element.sets_obj_id(R.get(ii).obj_id);
		    			  Element.sets_traj_id(R.get(ii).traj_id);
		    			  Element.sets_point(R.get(ii).point);
		    			  
		    			  if (R.get(i).point.t >= start && R.get(i).point.t <= end){
				    		  
		    				  if (NoMatch.contains(r)){
				    			  NoMatch.remove(r);
				    		  }

				    		  objPair = new DTJobjPair();
				    		  posPair = new DTJobjPair();
				
				    		  objPair.setr_obj_id(r.obj_id);
				    		  objPair.sets_obj_id(R.get(ii).obj_id);
				    		  posPair.setr_obj_id(i);
				    		  posPair.sets_obj_id(ii);
	
				    		  LastPairs.put(objPair, posPair);

		    				  context.write(Element, output_value);
		    				  
		    			  }
		    			  
		    			  Element.setr_obj_id(R.get(ii).obj_id);
		    			  Element.setr_traj_id(R.get(ii).traj_id);
		    			  Element.setr_point(R.get(ii).point);
		    			  Element.sets_obj_id(r.obj_id);
	    				  Element.sets_traj_id(r.traj_id);
	    				  Element.sets_point(r.point);
	    			 
		    			  if (R.get(ii).point.t >= start  && R.get(ii).point.t <= end){
		    				  
				    		  if (NoMatch.contains(R.get(ii))){
				    			  NoMatch.remove(R.get(ii));
				    		  }
				    		  
				    		  objPair = new DTJobjPair();
				    		  posPair = new DTJobjPair();
				
				    		  objPair.setr_obj_id(r.obj_id);
				    		  objPair.sets_obj_id(R.get(ii).obj_id);
				    		  posPair.setr_obj_id(i);
				    		  posPair.sets_obj_id(ii);
	
				    		  LastPairs.put(objPair, posPair);

				    		  context.write(Element, output_value);
		    				  
		    			  }
		    			  
		    			  if (dt > 0){
		    				  
				    		  pair_found2 = false;
				    		  
				    		  j_TrI_pos = Collections.binarySearch(TrI.get(R.get(ii).obj_id), ii);

				    		  if (j_TrI_pos > 0){
				    			  
				    			  j = TrI.get(R.get(ii).obj_id).get(j_TrI_pos - 1);
					    		  jj_TrI_pos = - Collections.binarySearch(TrI.get(R.get(i).obj_id), j) - 1;
					    		  if (jj_TrI_pos < TrI.get(R.get(i).obj_id).size() && jj_TrI_pos > -1){
						    		  jj = TrI.get(R.get(i).obj_id).get(jj_TrI_pos);
						    		  jj_SpI = jj_TrI_pos;
									
						    		  if (e_sp_method == 2){
						  	    		
								    		 epsilon_sp = (int)Math.floor(Mesh.get(R.get(j).cellID).cell.BoxSPDiameter() * epsilon_sp_prcnt);
								    		
								    	}

				    				  while ((jj_SpI < TrI.get(R.get(i).obj_id).size()) && (R.get(jj).getPoint().t - R.get(j).getPoint().t <= epsilon_t)){
				    					  if ((R.get(j).getPoint().toPointSP().DistanceSP(R.get(jj).getPoint().toPointSP()) <= epsilon_sp)) {
				    						  pair_found2 = true;
				    						  break;
				    					  }
				    					  jj_SpI++;
				    					  if (jj_SpI < TrI.get(R.get(i).obj_id).size()) {
					    					  jj = TrI.get(R.get(i).obj_id).get(jj_SpI);
				    					  }
				    				  }  
					    		  }

			    				  if (pair_found2 == true){
		
			    				  } else {
			    					  jj_TrI_pos = - Collections.binarySearch(TrI.get(R.get(i).obj_id), j) - 2;//binarySearch --> (-insertion_point - 1)
			    					  if (jj_TrI_pos < TrI.get(R.get(i).obj_id).size() && jj_TrI_pos > -1){
							    		  jj = TrI.get(R.get(i).obj_id).get(jj_TrI_pos);
				    					  jj_SpI = jj_TrI_pos;
							    		  
				    					  if (e_sp_method == 2){
								  	    		
								    		  epsilon_sp = (int)Math.floor(Mesh.get(R.get(j).cellID).cell.BoxSPDiameter() * epsilon_sp_prcnt);
									    		
									      }


					    				  while ((jj_SpI >= 0) && (R.get(j).getPoint().t - R.get(jj).getPoint().t <= epsilon_t)){
					    					  if ((R.get(j).getPoint().toPointSP().DistanceSP(R.get(jj).getPoint().toPointSP()) <= epsilon_sp)) {
					    						  pair_found2 = true;
					    						  break;
					    					  }
					    					  jj_SpI--;
					    					  if (jj_SpI >= 0) {
						    					  jj = TrI.get(R.get(i).obj_id).get(jj_SpI);
					    					  }
					    				  }
			    					  }
			    				  }
			    				  if (pair_found2 ==true){
		
			    				  } else {
		
			    		    		  match = false;
			    		    		  after = false;
			    		    		  sbldr.setLength(0);
						    		  
			    		    		  if (e_sp_method == 1){
											
							    		  sbldr.append(match).append(",").append(after);
									
						    		  } else if (e_sp_method == 2){
									
							    		  sbldr.append(match).append(",").append(after).append("|").append(Mesh.get(R.get(j).cellID).cell.BoxSPDiameter());
										
						    		  }
			    		    		  
			    		    		  
			    		    		  output_value.set(sbldr.toString());
			    					  
				    				  Element.setr_obj_id(r.obj_id);
				    				  Element.setr_traj_id(r.traj_id);
				    				  Element.setr_point(r.point);
					    			  Element.sets_obj_id(R.get(j).obj_id);
					    			  Element.sets_traj_id(R.get(j).traj_id);
					    			  Element.sets_point(R.get(j).point);
					    			 
					    			  context.write(Element, output_value);
							    				  
			    				  }
				    		  }
				    		  
				    		  
				    		  pair_found2 = false;
				    		  
				    		  j_TrI_pos = Collections.binarySearch(TrI.get(R.get(i).obj_id), i);

				    		  if (j_TrI_pos > 0){
				    			  
				    			  j = TrI.get(R.get(i).obj_id).get(j_TrI_pos - 1);
					    		  jj_TrI_pos = - Collections.binarySearch(TrI.get(R.get(ii).obj_id), j) - 1;
					    		  if (jj_TrI_pos < TrI.get(R.get(ii).obj_id).size() && jj_TrI_pos > -1){
						    		  jj = TrI.get(R.get(ii).obj_id).get(jj_TrI_pos);
						    		  jj_SpI = jj_TrI_pos;
			    					  
						    		  if (e_sp_method == 2){
							  	    		
							    		  epsilon_sp = (int)Math.floor(Mesh.get(R.get(j).cellID).cell.BoxSPDiameter() * epsilon_sp_prcnt);
								    		
								      }


				    				  while ((jj_SpI < TrI.get(R.get(ii).obj_id).size()) && (R.get(jj).getPoint().t - R.get(j).getPoint().t <= epsilon_t)){
				    					  if ((R.get(j).getPoint().toPointSP().DistanceSP(R.get(jj).getPoint().toPointSP()) <= epsilon_sp)) {
				    						  pair_found2 = true;
				    						  break;
				    					  }
				    					  jj_SpI++;
				    					  if (jj_SpI < TrI.get(R.get(ii).obj_id).size()) {
					    					  jj = TrI.get(R.get(ii).obj_id).get(jj_SpI);
				    					  }
				    				  }  
					    		  }
				    			  
					    		  if (pair_found2 == true){
			    				  
			    				  } else {
			    					  jj_TrI_pos = - Collections.binarySearch(TrI.get(R.get(ii).obj_id), j) - 2;//binarySearch --> (-insertion_point - 1)
			    					  if (jj_TrI_pos < TrI.get(R.get(ii).obj_id).size() && jj_TrI_pos > -1){
							    		  jj = TrI.get(R.get(ii).obj_id).get(jj_TrI_pos);
							    		  jj_SpI = jj_TrI_pos;
							    		  
							    		  if (e_sp_method == 2){
								  	    		
								    		  epsilon_sp = (int)Math.floor(Mesh.get(R.get(j).cellID).cell.BoxSPDiameter() * epsilon_sp_prcnt);
									    		
									      }


					    				  while ((jj_SpI >= 0) && (R.get(j).getPoint().t - R.get(jj).getPoint().t <= epsilon_t)){
					    					  if ((R.get(j).getPoint().toPointSP().DistanceSP(R.get(jj).getPoint().toPointSP()) <= epsilon_sp)) {
					    						  pair_found2 = true;
					    						  break;
					    					  }
					    					  jj_SpI--;
					    					  if (jj_SpI >= 0) {
						    					  jj = TrI.get(R.get(ii).obj_id).get(jj_SpI);
					    					  }
					    				  }
			    					  }
			    				  }

					    		  if (pair_found2 ==true){
			    				  } else {
		
			    		    		  match = false;
			    		    		  after = false;
			    		    		  sbldr.setLength(0);
			    		    		  
						    		  if (e_sp_method == 1){
											
							    		  sbldr.append(match).append(",").append(after);
									
						    		  } else if (e_sp_method == 2){
									
							    		  sbldr.append(match).append(",").append(after).append("|").append(Mesh.get(R.get(j).cellID).cell.BoxSPDiameter());
										
						    		  }
			    		    		  
			    		    		  output_value.set(sbldr.toString());
			    					  
				    				  Element.setr_obj_id(R.get(ii).obj_id);
				    				  Element.setr_traj_id(R.get(ii).traj_id);
				    				  Element.setr_point(R.get(ii).point);
					    			  Element.sets_obj_id(R.get(j).obj_id);
					    			  Element.sets_traj_id(R.get(j).traj_id);
					    			  Element.sets_point(R.get(j).point);
					    			 
					    			  context.write(Element, output_value);
						    				  
			    				  }
				    		  }

		    			  }

			    		  
			    	}
			    	
			    	ii_SpI_Pos--;

					if (ii_SpI_Pos >= 0) {
						ii = SpI.get(r.cellID).get(ii_SpI_Pos);
					}
					
	    		  }
			    }
	
				if (pair_found == false){
	
					if (R.get(i).point.t >= start  && R.get(i).point.t <= end){
						
						NoMatch.add(r);
						  
					}
				}
				
			    i++;
	        }
		}
		
		if (dt > 0){
			
			for (Map.Entry<DTJobjPair, DTJobjPair> entry : LastPairs.entrySet()) {

				DTJobjPair posPair = entry.getValue();

				i = posPair.r_obj_id;
				ii = posPair.s_obj_id;
			  
				j_TrI_pos = Collections.binarySearch(TrI.get(R.get(ii).obj_id), ii);
				pair_found2 = false;

				if (j_TrI_pos < TrI.get(R.get(ii).obj_id).size() - 1){
	  			  
					j = TrI.get(R.get(ii).obj_id).get(j_TrI_pos + 1);
		    		jj_TrI_pos = - Collections.binarySearch(TrI.get(R.get(i).obj_id), j) - 1;
		    		if (jj_TrI_pos < TrI.get(R.get(i).obj_id).size() && jj_TrI_pos > -1){
		    			jj = TrI.get(R.get(i).obj_id).get(jj_TrI_pos);
			    		jj_SpI = jj_TrI_pos;
			    		
			    		  if (e_sp_method == 2){
				  	    		
					    	epsilon_sp = (int)Math.floor(Mesh.get(R.get(j).cellID).cell.BoxSPDiameter() * epsilon_sp_prcnt);
					    		
					      }


		  				while (jj_SpI < TrI.get(R.get(i).obj_id).size() && (R.get(jj).getPoint().t - R.get(j).getPoint().t <= epsilon_t)){
	  						if ((R.get(j).getPoint().toPointSP().DistanceSP(R.get(jj).getPoint().toPointSP()) <= epsilon_sp)) {
	  							pair_found2 = true;
		    					break;
		    				}
		  					jj_SpI++;
		  					if (jj_SpI < TrI.get(R.get(i).obj_id).size()) {
		  						jj = TrI.get(R.get(i).obj_id).get(jj_SpI);
		  					}
		  				}  
		    		}

					if (pair_found2 == true){

					} else {
						jj_TrI_pos = - Collections.binarySearch(TrI.get(R.get(i).obj_id), j) - 2;//binarySearch --> (-insertion_point - 1)
						if (jj_TrI_pos < TrI.get(R.get(i).obj_id).size() && jj_TrI_pos > -1){
							jj = TrI.get(R.get(i).obj_id).get(jj_TrI_pos);
				    		jj_SpI = jj_TrI_pos;
				    		
				    		  if (e_sp_method == 2){
					  	    		
						    		epsilon_sp = (int)Math.floor(Mesh.get(R.get(j).cellID).cell.BoxSPDiameter() * epsilon_sp_prcnt);
							    		
							  }


		    				while ((jj_SpI >= 0) && (R.get(j).getPoint().t - R.get(jj).getPoint().t <= epsilon_t)){
	    						if ((R.get(j).getPoint().toPointSP().DistanceSP(R.get(jj).getPoint().toPointSP()) <= epsilon_sp)) {
		    						
	    							pair_found2 = true;
		    						break;
		    						
		    					}
		    					jj_SpI--;
		    					if (jj_SpI >= 0) {
		    						jj = TrI.get(R.get(i).obj_id).get(jj_SpI);
		    					}
		    				}
						}
					}
					if (pair_found2 ==true){

					} else {

						match = false;
			    		after = false;
			    		sbldr.setLength(0);
			    		
			    		if (e_sp_method == 1){
							
			    			sbldr.append(match).append(",").append(after);
					
			    		} else if (e_sp_method == 2){
					
			    			sbldr.append(match).append(",").append(after).append("|").append(Mesh.get(R.get(j).cellID).cell.BoxSPDiameter());
						
			    		}
			    		
			    		output_value.set(sbldr.toString());
						  
	    				Element.setr_obj_id(R.get(i).obj_id);
	    				Element.setr_traj_id(R.get(i).traj_id);
	      				Element.setr_point(R.get(i).point);
		    			Element.sets_obj_id(R.get(j).obj_id);
		    			Element.sets_traj_id(R.get(j).traj_id);
		    			Element.sets_point(R.get(j).point);
		    			 
		    			 
		    			context.write(Element, output_value);
		    				  
					}
	  		  	}
			}

		}
		
		
		for (DTJiPointElement2 nm : NoMatch){
			
			
			match = true;
			after = true;
			sbldr.setLength(0);
			
  		  	if (e_sp_method == 1){
				
  		  		sbldr.append(match).append(",").append(after);
		
  		  	} else if (e_sp_method == 2){
		
  		  		sbldr.append(match).append(",").append(after).append("|").append(0);
			
  		  	}
			
			output_value.set(sbldr.toString());
			
			Element.setr_obj_id(nm.obj_id);
			Element.setr_traj_id(nm.traj_id);
			Element.setr_point(nm.point);
			Element.sets_obj_id(0);
			Element.sets_traj_id(0);
			if(nm.point.p.n_of_dims() == 2){
				Element.sets_point(new PointST(0, new PointSP(0,0)));
			} else if (nm.point.p.n_of_dims() == 3){
				Element.sets_point(new PointST(0, new PointSP(0,0,0)));
			}
			
			context.write(Element, output_value);
		}
		
		cleanup(context);
	 
	}

	
}
