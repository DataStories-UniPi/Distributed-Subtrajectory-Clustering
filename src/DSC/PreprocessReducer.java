package DSC;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import CustomWritables.DTJrPointElement;



public class PreprocessReducer extends Reducer<DTJrPointElement, Text, Text, Text> {
	
	
	public void reduce(DTJrPointElement _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {


		DTJrPointElement Element = new DTJrPointElement();


	
		for (@SuppressWarnings("unused") Text val : values) {
			
			/*
				
				Some kind of further preprocessing can take place here
			
			*/
			
			Element = new DTJrPointElement(_key.obj_id, _key.traj_id, _key.point);

			context.write(new Text(String.valueOf(Element.point.t)), new Text(Element.toString()));

		}
	}

}
