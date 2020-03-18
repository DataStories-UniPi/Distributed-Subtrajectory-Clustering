package DSC;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import CustomWritables.DTJSubtraj;

public class JoinResultsMapper2 extends Mapper<LongWritable, Text, DTJSubtraj, Text> {
	

	String key_value = new String();
	StringTokenizer key_valuetokenizer = new StringTokenizer(key_value, "\t");
	String key = new String();
	StringTokenizer keytokenizer = new StringTokenizer(key, ",");
	String value = new String();
	StringTokenizer valuetokenizer = new StringTokenizer(value, ",");

	int r_obj_id;
	int r_traj_id;
	int r_subtraj_id;
	int s_obj_id;
	int s_traj_id;
	int s_subtraj_id;
	boolean repr_only;
    
	public void setup(Context context) throws IOException {
        
    	Configuration conf = context.getConfiguration();
        
    	repr_only = Boolean.parseBoolean(conf.get("repr_only"));
    	
    }
    
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		DTJSubtraj repr = new DTJSubtraj();
		DTJSubtraj key_subtraj = new DTJSubtraj();

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
		}
		
		repr = new DTJSubtraj(r_obj_id, r_traj_id, r_subtraj_id);
				
		valuetokenizer = new StringTokenizer(value, ",");
		
		while (valuetokenizer.hasMoreTokens()) {
			s_obj_id = Integer.parseInt(valuetokenizer.nextToken().trim());
			s_traj_id = Integer.parseInt(valuetokenizer.nextToken());
			s_subtraj_id = Integer.parseInt(valuetokenizer.nextToken());
			valuetokenizer.nextToken();

			key_subtraj = new DTJSubtraj(s_obj_id, s_traj_id, s_subtraj_id);
			
			if (repr_only && repr.equals(key_subtraj)){
				
				context.write(key_subtraj, new Text("Mapper2," + repr.toString()));

			} else if (!repr_only){
				
				context.write(key_subtraj, new Text("Mapper2," + repr.toString()));

			}


		}

		
	}

}
