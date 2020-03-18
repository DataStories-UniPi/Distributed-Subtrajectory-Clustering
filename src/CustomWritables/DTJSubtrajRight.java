package CustomWritables;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class DTJSubtrajRight implements Writable, WritableComparable<DTJSubtrajRight> {

	public int r_t;
	public int s_t;
	public double sim;

	public DTJSubtrajRight() {
		
	}

	public DTJSubtrajRight(int r_t, int s_t, double sim) {
		this.r_t = r_t;
		this.s_t = s_t;
		this.sim = sim;
	}

	@Override
	public String toString() {

		return (new StringBuilder().append(r_t).append(",").append(s_t).append(",").append(sim).toString());
	}

	public void readFields(DataInput dataInput) throws IOException {
		
		DoubleWritable sim_writable = new DoubleWritable();
		
		r_t = WritableUtils.readVInt(dataInput);
		s_t = WritableUtils.readVInt(dataInput);
		sim_writable.readFields(dataInput);
		sim = sim_writable.get();
	}

	public void write(DataOutput dataOutput) throws IOException {
		DoubleWritable sim_writable = new DoubleWritable();

		WritableUtils.writeVInt(dataOutput, r_t);
		WritableUtils.writeVInt(dataOutput, s_t);
		sim_writable.set(sim);
		sim_writable.write(dataOutput);
	}
	
	 @Override
	    public boolean equals(Object o) {

	        if (o == this) return true;
	        if (!(o instanceof DTJSubtrajRight)) {
	            return false;
	        }

	        DTJSubtrajRight pair = (DTJSubtrajRight) o;

	        return pair.r_t == r_t && pair.s_t == s_t;
	    }

	    //Idea from effective Java : Item 9
	    @Override
	    public int hashCode() {
	        int result = 17;
	        result = 31 * result + r_t;
	        result = 31 * result + s_t;
	        return result;
	    }
	    
	public int compareTo(DTJSubtrajRight objKeyPair) {


		int result = Integer.compare(r_t, objKeyPair.r_t);
		
		if (result == 0){
			result = Integer.compare(s_t, objKeyPair.s_t);
		}

		return result;
	}
	
	public int getr_t() {
		return r_t;
	}
	public void setr_t(int r_t) {
		this.r_t = r_t;
	}
	public int gets_t() {
		return s_t;
	}
	public void sets_t(int s_t) {
		this.s_t = s_t;
	}
	public double getsim() {
		return sim;
	}
	public void setsim(double sim) {
		this.sim = sim;
	}


}