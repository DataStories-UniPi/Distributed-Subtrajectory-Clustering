package CustomWritables;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class DTJSubtrajVot implements Writable, WritableComparable<DTJSubtrajVot> {

	public int obj_id;
	public int traj_id;
	public int subtraj_id;
	public double voting;

	public DTJSubtrajVot() {
		
	}

	public DTJSubtrajVot(int obj_id, int traj_id, int subtraj_id, double voting) {
		this.obj_id = obj_id;
		this.traj_id = traj_id;
		this.subtraj_id = subtraj_id;
		this.voting = voting;
	}

	@Override
	public String toString() {

		return (new StringBuilder().append(obj_id).append(",").append(traj_id).append(",").append(subtraj_id).append(",").append(voting).toString());
	}

	public void readFields(DataInput dataInput) throws IOException {
		DoubleWritable voting_writable = new DoubleWritable();

		obj_id = WritableUtils.readVInt(dataInput);
		traj_id = WritableUtils.readVInt(dataInput);
		subtraj_id = WritableUtils.readVInt(dataInput);
		voting_writable.readFields(dataInput);
		voting = voting_writable.get();
	}

	public void write(DataOutput dataOutput) throws IOException {
		DoubleWritable voting_writable = new DoubleWritable();

		WritableUtils.writeVInt(dataOutput, obj_id);
		WritableUtils.writeVInt(dataOutput, traj_id);
		WritableUtils.writeVInt(dataOutput, subtraj_id);
		voting_writable.set(voting);
		voting_writable.write(dataOutput);
	}
	
	 @Override
	    public boolean equals(Object o) {

	        if (o == this) return true;
	        if (!(o instanceof DTJSubtraj)) {
	            return false;
	        }

	        DTJSubtraj pair = (DTJSubtraj) o;

	        return pair.obj_id == obj_id && pair.traj_id == traj_id && pair.subtraj_id == subtraj_id;
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
	    
	public int compareTo(DTJSubtrajVot objKeyPair) {

		int result = Double.compare(voting, objKeyPair.voting);
		if (result == 0){
			 
			result =Integer.compare(obj_id, objKeyPair.obj_id);
		}
		
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
	public double getvoting() {
		return voting;
	}
	public void setvoting(double voting) {
		this.voting = voting;
	}

}