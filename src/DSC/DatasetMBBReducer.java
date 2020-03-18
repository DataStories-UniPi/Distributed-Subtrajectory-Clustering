package DSC;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import DataTypes.BoxST;
import DataTypes.PointSP;
import DataTypes.PointST;


public class DatasetMBBReducer extends Reducer<Text, Text, NullWritable, Text> {
	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String line = new String();
		StringTokenizer linetokenizer = new StringTokenizer(line, ",");
	    PointST lp = new PointST();
	    PointST hp = new PointST();
	    BoxST DatasetMBB = new BoxST();
	    
	    int n_of_args = 0;
	    
	    int t_max = 0;
	    int t_min = 0;
	    int x_max = 0;
	    int x_min = 0;
	    int y_max = 0;
	    int y_min = 0;
	    int z_max = 0;
	    int z_min = 0;

	    int max_t_max = 0;
	    int min_t_min = Integer.MAX_VALUE;
	    int max_x_max = Integer.MIN_VALUE;
	    int min_x_min = Integer.MAX_VALUE;
	    int max_y_max = Integer.MIN_VALUE;
	    int min_y_min = Integer.MAX_VALUE;
	    int max_z_max = Integer.MIN_VALUE;
	    int min_z_min = Integer.MAX_VALUE;

	    
		for (Text val : values) {
			
			System.out.println("val: " + val);
			
			line = val.toString();
			linetokenizer = new StringTokenizer(line, ",");
			n_of_args = linetokenizer.countTokens();
			
			System.out.println("n_of_args: " + n_of_args);
			
			if (n_of_args == 6){
			    while (linetokenizer.hasMoreTokens()) {

			    	t_min = Integer.parseInt(linetokenizer.nextToken());
			    	x_min = Integer.parseInt(linetokenizer.nextToken());
			    	y_min = Integer.parseInt(linetokenizer.nextToken());
			    	t_max = Integer.parseInt(linetokenizer.nextToken());
			    	x_max = Integer.parseInt(linetokenizer.nextToken());
			    	y_max = Integer.parseInt(linetokenizer.nextToken());

			    }

		    	if (t_max > max_t_max){
		    		max_t_max = t_max;
		    	}
		    	if (t_min < min_t_min){
		    		min_t_min = t_min;
		    	}
		    	if (x_max > max_x_max){
		    		max_x_max = x_max;
		    	}
		    	if (x_min < min_x_min){
		    		min_x_min = x_min;
		    	}
		    	if (y_max > max_y_max){
		    		max_y_max = y_max;
		    	}
		    	if (y_min < min_y_min){
		    		min_y_min = y_min;
		    	}

			} else if (n_of_args == 8){
			    while (linetokenizer.hasMoreTokens()) {
			    	t_min = Integer.parseInt(linetokenizer.nextToken());
			    	x_min = Integer.parseInt(linetokenizer.nextToken());
			    	y_min = Integer.parseInt(linetokenizer.nextToken());
			    	z_min = Integer.parseInt(linetokenizer.nextToken());
			    	t_max = Integer.parseInt(linetokenizer.nextToken());
			    	x_max = Integer.parseInt(linetokenizer.nextToken());
			    	y_max = Integer.parseInt(linetokenizer.nextToken());
			    	z_max = Integer.parseInt(linetokenizer.nextToken());
			    }
			    
		    	if (t_max > max_t_max){
		    		max_t_max = t_max;
		    	}
		    	if (t_min < min_t_min){
		    		min_t_min = t_min;
		    	}
		    	if (x_max > max_x_max){
		    		max_x_max = x_max;
		    	}
		    	if (x_min < min_x_min){
		    		min_x_min = x_min;
		    	}
		    	if (y_max > max_y_max){
		    		max_y_max = y_max;
		    	}
		    	if (y_min < min_y_min){
		    		min_y_min = y_min;
		    	}
		    	if (z_max > max_z_max){
		    		max_z_max = z_max;
		    	}
		    	if (z_min < min_z_min){
		    		min_z_min = z_min;
		    	}
			}

	    	
    	}
		
		if (n_of_args == 6){
			lp = new PointST(min_t_min, new PointSP(min_x_min, min_y_min));
			hp = new PointST(max_t_max, new PointSP(max_x_max, max_y_max));
		} else if (n_of_args == 8){
			lp = new PointST(min_t_min, new PointSP(min_x_min, min_y_min, min_z_min));
			hp = new PointST(max_t_max, new PointSP(max_x_max, max_y_max, max_z_max));
		}
		
		DatasetMBB.setBoxST(lp, hp);

	    context.write(NullWritable.get(), new Text(DatasetMBB.toString()));
	        	
	}
}
