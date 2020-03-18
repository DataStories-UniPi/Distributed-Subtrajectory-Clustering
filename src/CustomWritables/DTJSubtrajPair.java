package CustomWritables;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class DTJSubtrajPair implements Writable, WritableComparable<DTJSubtrajPair> {

	public int r_obj_id;
	public int r_traj_id;
	public int r_subtraj_id;
	public int s_obj_id;
	public int s_traj_id;
	public int s_subtraj_id;

	public DTJSubtrajPair() {
		
	}

	public DTJSubtrajPair(int r_obj_id, int r_traj_id, int r_subtraj_id, int s_obj_id, int s_traj_id, int s_subtraj_id) {
		this.r_obj_id = r_obj_id;
		this.r_traj_id = r_traj_id;
		this.r_subtraj_id = r_subtraj_id;
		this.s_obj_id = s_obj_id;
		this.s_traj_id = s_traj_id;
		this.s_subtraj_id = s_subtraj_id;

	}
	
	public DTJSubtrajPair(DTJSubtrajPair elem) {
		this.r_obj_id = elem.r_obj_id;
		this.r_traj_id = elem.r_traj_id;
		this.r_subtraj_id = elem.r_subtraj_id;
		this.s_obj_id = elem.s_obj_id;
		this.s_traj_id = elem.s_traj_id;
		this.s_subtraj_id = elem.s_subtraj_id;
	}


	@Override
	public String toString() {

		return (new StringBuilder().append(r_obj_id).append(",").append(r_traj_id).append(",").append(r_subtraj_id).append(",").append(s_obj_id).append(",").append(s_traj_id).append(",").append(s_subtraj_id).toString());
	}

	public void readFields(DataInput dataInput) throws IOException {
		
		r_obj_id = WritableUtils.readVInt(dataInput);
		r_traj_id = WritableUtils.readVInt(dataInput);
		r_subtraj_id = WritableUtils.readVInt(dataInput);
		s_obj_id = WritableUtils.readVInt(dataInput);
		s_traj_id = WritableUtils.readVInt(dataInput);
		s_subtraj_id = WritableUtils.readVInt(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		
		WritableUtils.writeVInt(dataOutput, r_obj_id);
		WritableUtils.writeVInt(dataOutput, r_traj_id);
		WritableUtils.writeVInt(dataOutput, r_subtraj_id);
		WritableUtils.writeVInt(dataOutput, s_obj_id);
		WritableUtils.writeVInt(dataOutput, s_traj_id);
		WritableUtils.writeVInt(dataOutput, s_subtraj_id);
	}
	
	 @Override
	    public boolean equals(Object o) {

	        if (o == this) return true;
	        if (!(o instanceof DTJSubtrajPair)) {
	            return false;
	        }

	        DTJSubtrajPair pair = (DTJSubtrajPair) o;

	        return pair.r_obj_id == r_obj_id && pair.r_traj_id == r_traj_id && pair.r_subtraj_id == r_subtraj_id && pair.s_obj_id == s_obj_id && pair.s_traj_id == s_traj_id && pair.s_subtraj_id == s_subtraj_id;
	    }

	    //Idea from effective Java : Item 9
	    @Override
	    public int hashCode() {
	        int result = 17;
	        result = 31 * result + r_obj_id;
	        result = 31 * result + r_traj_id;
	        result = 31 * result + r_subtraj_id;
	        result = 31 * result + s_obj_id;
	        result = 31 * result + s_traj_id;
	        result = 31 * result + s_subtraj_id;
	        return result;
	    }
	    
	public int compareTo(DTJSubtrajPair objKeyPair) {

		int result = Integer.compare(r_obj_id, objKeyPair.r_obj_id);
		
		if (result == 0){
			 
			result =Integer.compare(r_traj_id, objKeyPair.r_traj_id);
		}
		if (result == 0){
 
			result =Integer.compare(r_subtraj_id, objKeyPair.r_subtraj_id);
		}
		if (result == 0){
 
			result =Integer.compare(s_obj_id, objKeyPair.s_obj_id);
		}
		
		if (result == 0){
 
			result =Integer.compare(s_traj_id, objKeyPair.s_traj_id);
		}
		if (result == 0){
			 
			result =Integer.compare(s_subtraj_id, objKeyPair.s_subtraj_id);
		}

		return result;
	}

	public int getr_obj_id() {
		return r_obj_id;
	}
	public void setr_obj_id(int r_obj_id) {
		this.r_obj_id = r_obj_id;
	}
	public int getr_traj_id() {
		return r_traj_id;
	}
	public void setr_traj_id(int r_traj_id) {
		this.r_traj_id = r_traj_id;
	}
	public int getr_subtraj_id() {
		return r_subtraj_id;
	}
	public void setr_subtraj_id(int r_subtraj_id) {
		this.r_subtraj_id = r_subtraj_id;
	}
	public int gets_obj_id() {
		return s_obj_id;
	}
	public void sets_obj_id(int s_obj_id) {
		this.s_obj_id = s_obj_id;
	}
	public int gets_traj_id() {
		return s_traj_id;
	}
	public void sets_traj_id(int s_traj_id) {
		this.s_traj_id = s_traj_id;
	}
	public int gets_subtraj_id() {
		return s_subtraj_id;
	}
	public void sets_subtraj_id(int s_subtraj_id) {
		this.s_subtraj_id = s_subtraj_id;
	}
}