package CustomWritables;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class DTJSubtrajSim implements Writable, WritableComparable<DTJSubtrajSim> {

	public int obj_id;
	public int traj_id;
	public int subtraj_id;
	public double sim;
	//public double rmse;

	public DTJSubtrajSim() {
		
	}

	
	public DTJSubtrajSim(int obj_id, int traj_id, int subtraj_id, double sim/*, double rmse*/) {
		this.obj_id = obj_id;
		this.traj_id = traj_id;
		this.subtraj_id = subtraj_id;
		this.sim = sim;
		//this.rmse = rmse;

	}

	public DTJSubtrajSim(DTJSubtrajSim element) {
		this.obj_id = element.obj_id;
		this.traj_id = element.traj_id;
		this.subtraj_id = element.subtraj_id;
		this.sim = element.sim;
		//this.rmse = element.rmse;

	}

	@Override
	public String toString() {

		return (new StringBuilder().append(obj_id).append(",").append(traj_id).append(",").append(subtraj_id).append(",").append(sim)/*.append(",").append(rmse)*/.toString());
	}

	public void readFields(DataInput dataInput) throws IOException {
		DoubleWritable sim_writable = new DoubleWritable();
		//DoubleWritable rmse_writable = new DoubleWritable();

		obj_id = WritableUtils.readVInt(dataInput);
		traj_id = WritableUtils.readVInt(dataInput);
		subtraj_id = WritableUtils.readVInt(dataInput);
		sim_writable.readFields(dataInput);
		sim = sim_writable.get();
		//rmse_writable.readFields(dataInput);
		//rmse = rmse_writable.get();

	}

	public void write(DataOutput dataOutput) throws IOException {
		DoubleWritable sim_writable = new DoubleWritable();
		//DoubleWritable rmse_writable = new DoubleWritable();

		WritableUtils.writeVInt(dataOutput, obj_id);
		WritableUtils.writeVInt(dataOutput, traj_id);
		WritableUtils.writeVInt(dataOutput, subtraj_id);
		sim_writable.set(sim);
		sim_writable.write(dataOutput);
		//rmse_writable.set(rmse);
		//rmse_writable.write(dataOutput);
	}
	
	 @Override
	    public boolean equals(Object o) {

	        if (o == this) return true;
	        if ((o instanceof DTJSubtrajSim)) {
		        
	        	DTJSubtrajSim pair = (DTJSubtrajSim) o;
		        return pair.obj_id == obj_id && pair.traj_id == traj_id && pair.subtraj_id == subtraj_id;
		        
	        } else if ((o instanceof DTJSubtraj)){
	        	
	        	DTJSubtraj pair = (DTJSubtraj) o;
		        return pair.obj_id == obj_id && pair.traj_id == traj_id && pair.subtraj_id == subtraj_id;
  	
	        } else {
	        	return false;
	        }

	    }

	    //Idea from effective Java : Item 9
	    @Override
	    public int hashCode() {
	        int result = 17;
	        result = 31 * result + obj_id;
	        result = 31 * result + traj_id;
	        result = 31 * result + subtraj_id;

	        return result;
	    }
	    
	public int compareTo(DTJSubtrajSim objKeyPair) {

		int result = Integer.compare(obj_id, objKeyPair.obj_id);
		
		if (result == 0){
			 
			result =Integer.compare(traj_id, objKeyPair.traj_id);
		}
		
		if (result == 0){
 
			result =Integer.compare(subtraj_id, objKeyPair.subtraj_id);
		}
		

		return result;
	}

	public int getobj_id() {
		return obj_id;
	}
	public void setobj_id(int obj_id) {
		this.obj_id = obj_id;
	}
	public int gettraj_id() {
		return traj_id;
	}
	public void settraj_id(int traj_id) {
		this.traj_id = traj_id;
	}
	public int getsubtraj_id() {
		return subtraj_id;
	}
	public void setsubtraj_id(int subtraj_id) {
		this.subtraj_id = subtraj_id;
	}
	public double getsim() {
		return sim;
	}
	public void setsim(double sim) {
		this.sim = sim;
	}
	/*public double getrmse() {
		return rmse;
	}
	public void setrmse(double rmse) {
		this.rmse = rmse;
	}*/

	public DTJSubtraj toDTJSubtraj() {
		
		return new DTJSubtraj(this.obj_id, this.traj_id, this.subtraj_id);
		
	}

}