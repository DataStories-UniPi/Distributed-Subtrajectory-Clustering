package DSC;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




public class DatasetMBBMapper extends Mapper<LongWritable, Text, Text, Text> {
 
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {

		context.write(new Text(NullWritable.get().toString()), ivalue);
	}
	
	
}
