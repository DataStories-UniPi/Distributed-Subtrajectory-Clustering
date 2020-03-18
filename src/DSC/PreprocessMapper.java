package DSC;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.cts.CRSFactory;
import org.cts.IllegalCoordinateException;
import org.cts.crs.CRSException;
import org.cts.crs.GeodeticCRS;
import org.cts.op.CoordinateOperation;
import org.cts.op.CoordinateOperationFactory;
import org.cts.registry.EPSGRegistry;
import org.cts.registry.RegistryManager;

import CustomWritables.DTJrPointElement;
import DataTypes.PointSP;
import DataTypes.PointST;


public class PreprocessMapper extends Mapper<LongWritable, Text, DTJrPointElement, Text> {
	DTJrPointElement output_key = new DTJrPointElement();
	String line = new String();
    StringTokenizer linetokenizer = new StringTokenizer(line, ",");
    int obj_id = 0;
    int traj_id = 0; 

    int n_of_tokens = 0;
    
    int t = 0;
    int alt = 0;
    Double lon;
    Double lat;
    int x;
    int y;
    double[] dd;
    double[] coord = new double[2];
    List<CoordinateOperation> coordOps = new ArrayList<CoordinateOperation>();
    
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
  
        line = ivalue.toString();
        linetokenizer = new StringTokenizer(line, ",");
        n_of_tokens = linetokenizer.countTokens();
        
        if (n_of_tokens == 4){
        	
            while (linetokenizer.hasMoreTokens()) {

            	obj_id = Integer.parseInt(linetokenizer.nextToken());
            	traj_id = 1;
            	t = Integer.parseInt(linetokenizer.nextToken());
            	lon = Double.parseDouble(linetokenizer.nextToken());
            	lat = Double.parseDouble(linetokenizer.nextToken());
            }
            
	        coord[0] = lon;
	        coord[1] = lat;
	
	
	        try {
	
			    if (coordOps.size() != 0) {
		    		CoordinateOperation op = coordOps.get(0);
		    		dd  = op.transform(coord);
			    }
	
			} catch (IllegalCoordinateException e) {
				e.printStackTrace();
			}
	        
	        x = (int) Math.round(dd[0]);
	        y = (int) Math.round(dd[1]);
	
	        output_key = new DTJrPointElement(obj_id, traj_id, new PointST(t, new PointSP(x, y)));
	        
	        	
	        context.write(output_key, new Text());

            
        } else if (n_of_tokens == 5){
        	
            while (linetokenizer.hasMoreTokens()) {

            	obj_id = Integer.parseInt(linetokenizer.nextToken());
            	traj_id = Integer.parseInt(linetokenizer.nextToken());
            	t = Integer.parseInt(linetokenizer.nextToken());
            	lon = Double.parseDouble(linetokenizer.nextToken());
            	lat = Double.parseDouble(linetokenizer.nextToken());
            }
            
	        coord[0] = lon;
	        coord[1] = lat;
	
	
	        try {
	
			    if (coordOps.size() != 0) {
		    		CoordinateOperation op = coordOps.get(0);
		    		dd  = op.transform(coord);
			    }
	
			} catch (IllegalCoordinateException e) {
				e.printStackTrace();
			}
	        
	        x = (int) Math.round(dd[0]);
	        y = (int) Math.round(dd[1]);
	
	        output_key = new DTJrPointElement(obj_id, traj_id, new PointST(t, new PointSP(x, y)));
	        
	        	
	        context.write(output_key, new Text());

            
        } else if (n_of_tokens == 6){
        	
            while (linetokenizer.hasMoreTokens()) {

	        	obj_id = Integer.parseInt(linetokenizer.nextToken());
	        	traj_id = Integer.parseInt(linetokenizer.nextToken());
	        	t = Integer.parseInt(linetokenizer.nextToken());
	        	lon = Double.parseDouble(linetokenizer.nextToken());
	        	lat = Double.parseDouble(linetokenizer.nextToken());
	        	alt = Integer.parseInt(linetokenizer.nextToken());
            
            }
            
	        coord[0] = lon;
	        coord[1] = lat;
	
	
	        try {
	
			    if (coordOps.size() != 0) {
		    		CoordinateOperation op = coordOps.get(0);
		    		dd  = op.transform(coord);
			    }
	
			} catch (IllegalCoordinateException e) {
				e.printStackTrace();
			}
	        
	        x = (int) Math.round(dd[0]);
	        y = (int) Math.round(dd[1]);
	
	        output_key = new DTJrPointElement(obj_id, traj_id, new PointST(t, new PointSP(x, y, alt)));
	        
	        	
	        context.write(output_key, new Text());

	        
        }
            


	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {

		setup(context);

		try {
			
	        CRSFactory cRSFactory = new CRSFactory();
	        RegistryManager registryManager = cRSFactory.getRegistryManager();
	        registryManager.addRegistry(new EPSGRegistry());
	        GeodeticCRS  crs1 = (GeodeticCRS) cRSFactory.getCRS("EPSG:4326");
	        GeodeticCRS  crs2 = (GeodeticCRS) cRSFactory.getCRS("EPSG:3857");

	 
	        coordOps = CoordinateOperationFactory.createCoordinateOperations(crs1,crs2);
		}
		
        catch (CRSException e){
            e.printStackTrace();
        }
		
		while (context.nextKeyValue()) {

			map(context.getCurrentKey(), context.getCurrentValue(), context);
			
		}
		cleanup(context);
		 
	}


}
