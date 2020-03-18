package DSC;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import DataTypes.PointSP;
import DataTypes.PointST;


public class EquiDepthHistMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	IntWritable output_key = new IntWritable();
	Text output_value = new Text();
	String line = new String();
    StringTokenizer linetokenizer = new StringTokenizer(line, ",");
    int obj_id = 0; 
    int traj_id = 0;
    PointST point = new PointST();
    int n_of_args = 0;

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {


        
        //System.out.println("Input: " + ivalue);
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

        
        output_key.set(point.t);
        output_value.set(obj_id+","+traj_id+","+point.toString());
        
    	context.write(output_key, output_value);
    	//System.out.println("Output: " + output_key + "  " + output_value);

	}

}
