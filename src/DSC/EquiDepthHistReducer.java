package DSC;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.ReflectionUtils;

public class EquiDepthHistReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
	
	List<Integer> borders = new ArrayList<Integer>();

	 private MultipleOutputs<NullWritable, Text> out;

	    @Override
	    public void setup(Context context) throws IOException {
	    	
	        out = new MultipleOutputs<NullWritable, Text>(context);
	        
	        Configuration conf = context.getConfiguration();
	        URI[] cacheFile = context.getCacheFiles();
	       	Path path = new Path(cacheFile[0]);
	       	SequenceFile.Reader reader = null;
	        reader = new SequenceFile.Reader(conf, Reader.file(path), Reader.bufferSize(4096), Reader.start(0));
	        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
	        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
     	
	        while (reader.next(key, value)) {
     		   	borders.add(Integer.valueOf(key.toString()));
     		}
  
	        IOUtils.closeStream(reader);
	    }
	    // 
	    // You must override the cleanup method and close the multi-output object
	    // or things do not work correctly.
	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	        out.close();
	    }
	    
	public void reduce(IntWritable _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		int reducerID = context.getTaskAttemptID().getTaskID().getId();
		
		for (Text val : values) {
		    if(borders.size()==0){
	        	out.write(NullWritable.get(), val, "0" + " " + String.valueOf(Integer.MAX_VALUE));

		    } else	if (reducerID == 0){
	        	out.write(NullWritable.get(), val, "0" + " " + String.valueOf(borders.get(reducerID)-1));
	        } else if (reducerID == borders.size()){
	        	out.write(NullWritable.get(), val, String.valueOf(borders.get(reducerID-1)) + " " + String.valueOf(Integer.MAX_VALUE));
	        } else {
		        out.write(NullWritable.get(), val, String.valueOf(borders.get(reducerID-1))+" "+String.valueOf(borders.get(reducerID)-1));
	        }
		}

	}

}
