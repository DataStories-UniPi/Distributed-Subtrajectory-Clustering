package DSC;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import DataTypes.BoxST;
import DataTypes.PointSP;
import DataTypes.PointST;


public class DatasetMBBCombiner extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		
		String line = new String();
		StringTokenizer linetokenizer = new StringTokenizer(line, ",");
	    PointST point = new PointST();
	    
	    int t = 0;
	    int x = 0;
	    int y = 0;
	    int z = 0;
	    PointST lp = new PointST();
	    PointST hp = new PointST();
	    BoxST DatasetMBB = new BoxST();

	    int n_of_args = 0;
	    
	    int max_t_max = 0;
	    int min_t_min = Integer.MAX_VALUE;
	    int max_x_max = Integer.MIN_VALUE;
	    int min_x_min = Integer.MAX_VALUE;
	    int max_y_max = Integer.MIN_VALUE;
	    int min_y_min = Integer.MAX_VALUE;
	    int max_z_max = Integer.MIN_VALUE;
	    int min_z_min = Integer.MAX_VALUE;

	    
		for (Text val : values) {

			line = val.toString();
			linetokenizer = new StringTokenizer(line, ",");
			n_of_args = linetokenizer.countTokens();
			
			if (n_of_args == 5){
			    while (linetokenizer.hasMoreTokens()) {

			    	linetokenizer.nextToken();
			    	linetokenizer.nextToken();
			    	t = Integer.parseInt(linetokenizer.nextToken());
			    	x = Integer.parseInt(linetokenizer.nextToken());
			    	y = Integer.parseInt(linetokenizer.nextToken());

			    }
			    
			    point = new PointST(t, new PointSP(x, y));

		    	if (point.t > max_t_max){
		    		max_t_max = point.t;
		    	}
		    	if (point.t < min_t_min){
		    		min_t_min = point.t;
		    	}
		    	if (point.p.x > max_x_max){
		    		max_x_max = point.p.x;
		    	}
		    	if (point.p.x < min_x_min){
		    		min_x_min = point.p.x;
		    	}
		    	if (point.p.y > max_y_max){
		    		max_y_max = point.p.y;
		    	}
		    	if (point.p.y < min_y_min){
		    		min_y_min = point.p.y;
		    	}
			} else if (n_of_args == 6){
			    while (linetokenizer.hasMoreTokens()) {

			    	linetokenizer.nextToken();
			    	linetokenizer.nextToken();
			    	t = Integer.parseInt(linetokenizer.nextToken());
			    	x = Integer.parseInt(linetokenizer.nextToken());
			    	y = Integer.parseInt(linetokenizer.nextToken());
			    	z = Integer.parseInt(linetokenizer.nextToken());
			    }
			    
			    point = new PointST(t, new PointSP(x, y, z));

		    	if (point.t > max_t_max){
		    		max_t_max = point.t;
		    	}
		    	if (point.t < min_t_min){
		    		min_t_min = point.t;
		    	}
		    	if (point.p.x > max_x_max){
		    		max_x_max = point.p.x;
		    	}
		    	if (point.p.x < min_x_min){
		    		min_x_min = point.p.x;
		    	}
		    	if (point.p.y > max_y_max){
		    		max_y_max = point.p.y;
		    	}
		    	if (point.p.y < min_y_min){
		    		min_y_min = point.p.y;
		    	}
		    	if (point.p.z > max_z_max){
		    		max_z_max = point.p.z;
		    	}
		    	if (point.p.z < min_z_min){
		    		min_z_min = point.p.z;
		    	}
			}

	    	
    	}
		
		if (n_of_args == 5){
			lp = new PointST(min_t_min, new PointSP(min_x_min, min_y_min));
			hp = new PointST(max_t_max, new PointSP(max_x_max, max_y_max));
		} else if (n_of_args == 6){
			lp = new PointST(min_t_min, new PointSP(min_x_min, min_y_min, min_z_min));
			hp = new PointST(max_t_max, new PointSP(max_x_max, max_y_max, max_z_max));
		}

		DatasetMBB.setBoxST(lp, hp);

		context.write(_key, new Text(DatasetMBB.toString()));
	        	
	}
}
