package DSC;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.cts.CRSFactory;
import org.cts.IllegalCoordinateException;
import org.cts.crs.CRSException;
import org.cts.crs.GeodeticCRS;
import org.cts.op.CoordinateOperation;
import org.cts.op.CoordinateOperationFactory;
import org.cts.registry.EPSGRegistry;
import org.cts.registry.RegistryManager;

import CustomWritables.DTJSubtraj;
import DataTypes.PointSP;
import DataTypes.PointST;


public class JoinResultsReducer extends Reducer<DTJSubtraj, Text, Text, Text> {
	
	String value = new String();
	StringTokenizer valuetokenizer = new StringTokenizer(value, ",");
	String source = new String();
	int obj_id;
	int traj_id;
	int subtraj_id;
    int t = 0;
    int x = 0;
    int y = 0;
    PointST point = new PointST();
    DTJSubtraj subtraj = new DTJSubtraj();
    int n_of_args = 0;
    boolean repr_only;
    
    ArrayList<PointST> pointlist = new ArrayList<PointST>();
    
    double[] coord = new double[2];
    double[] dd;
    List<CoordinateOperation> coordOps = new ArrayList<CoordinateOperation>();

    public void setup(Context context) throws IOException {
        
    	Configuration conf = context.getConfiguration();
        
    	repr_only = Boolean.parseBoolean(conf.get("repr_only"));
    	
    }
    
	public void reduce(DTJSubtraj _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		pointlist.clear();
		subtraj = new DTJSubtraj(-1,-1,-1);
		
		try {
	        CRSFactory cRSFactory = new CRSFactory();
	        RegistryManager registryManager = cRSFactory.getRegistryManager();
	        registryManager.addRegistry(new EPSGRegistry());
	        GeodeticCRS  crs1 = (GeodeticCRS) cRSFactory.getCRS("EPSG:3857");
	        GeodeticCRS  crs2 = (GeodeticCRS) cRSFactory.getCRS("EPSG:4326");

	 
	        coordOps = CoordinateOperationFactory.createCoordinateOperations(crs1,crs2);
		}
        catch (CRSException e){
            e.printStackTrace();
        }
		
		for (Text val : values) {
			
			value = val.toString();
			valuetokenizer = new StringTokenizer(value, ",");
			n_of_args = valuetokenizer.countTokens();

			while (valuetokenizer.hasMoreTokens()) {
				source = valuetokenizer.nextToken();

				if(source.equals("Mapper1")){
					
					if (n_of_args == 4){
							
						point = new PointST(Integer.parseInt(valuetokenizer.nextToken()), new PointSP(Integer.parseInt(valuetokenizer.nextToken()), Integer.parseInt(valuetokenizer.nextToken())));
						
					} else if (n_of_args == 5) {
						
						point = new PointST(Integer.parseInt(valuetokenizer.nextToken()), new PointSP(Integer.parseInt(valuetokenizer.nextToken()), Integer.parseInt(valuetokenizer.nextToken()), Integer.parseInt(valuetokenizer.nextToken())));
						
					}
					
		        	pointlist.add(point);
		        	
				} else if(source.equals("Mapper2")){
					
					obj_id = Integer.parseInt(valuetokenizer.nextToken());
					traj_id = Integer.parseInt(valuetokenizer.nextToken());
					subtraj_id = Integer.parseInt(valuetokenizer.nextToken());
					
					subtraj = new DTJSubtraj(obj_id, traj_id, subtraj_id);

				}
				
			}
			
		}
		
		for(int i = 0; i < pointlist.size(); i++){
			
	        coord[0] = (double)pointlist.get(i).p.x;
	        coord[1] = (double)pointlist.get(i).p.y;

            try {

    		    if (coordOps.size() != 0) {
		    		CoordinateOperation op = coordOps.get(0);
		    		dd  = op.transform(coord);
    		    }

			} catch (IllegalCoordinateException e) {

				e.printStackTrace();
			}
			
	        if (repr_only && subtraj.equals(_key)){

				context.write(new Text(subtraj.obj_id + "_" + subtraj.traj_id + "_" + subtraj.subtraj_id), new Text(_key.obj_id + "_" + _key.traj_id + "_" + _key.subtraj_id + "," + pointlist.get(i).toString() + "," + dd[0] + "," + dd[1]));

			} else if (!repr_only){
				
				context.write(new Text(subtraj.obj_id + "_" + subtraj.traj_id + "_" + subtraj.subtraj_id), new Text(_key.obj_id + "_" + _key.traj_id + "_" + _key.subtraj_id + "," + pointlist.get(i).toString() + "," + dd[0] + "," + dd[1]));

			}
				
			
			
		}
		
	}

}
