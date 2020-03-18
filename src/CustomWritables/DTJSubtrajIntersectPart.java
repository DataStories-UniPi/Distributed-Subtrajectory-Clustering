package CustomWritables;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class DTJSubtrajIntersectPart implements Writable, WritableComparable<DTJSubtrajIntersectPart> {

	public int obj_id;
	public int traj_id;
	public int subtraj_id;
	public int t_min;

	public DTJSubtrajIntersectPart() {
		
	}

	public DTJSubtrajIntersectPart(int obj_id, int traj_id, int subtraj_id, int t_min) {
		this.obj_id = obj_id;
		this.traj_id = traj_id;
		this.subtraj_id = subtraj_id;
		this.t_min = t_min;
	}
	
	public DTJSubtrajIntersectPart(DTJSubtrajIntersectPart element) {
		this.obj_id = element.obj_id;
		this.traj_id = element.traj_id;
		this.subtraj_id = element.subtraj_id;
		this.t_min = element.t_min;
	}

	@Override
	public String toString() {

		return (new StringBuilder().append(obj_id).append(",").append(traj_id).append(",").append(subtraj_id).append(",").append(t_min).toString());
	}

	public void readFields(DataInput dataInput) throws IOException {
		
		obj_id = WritableUtils.readVInt(dataInput);
		traj_id = WritableUtils.readVInt(dataInput);
		subtraj_id = WritableUtils.readVInt(dataInput);
		t_min = WritableUtils.readVInt(dataInput);

	}

	public void write(DataOutput dataOutput) throws IOException {
		
		WritableUtils.writeVInt(dataOutput, obj_id);
		WritableUtils.writeVInt(dataOutput, traj_id);
		WritableUtils.writeVInt(dataOutput, subtraj_id);
		WritableUtils.writeVInt(dataOutput, t_min);

	}
	

	 @Override
	    public boolean equals(Object o) {

	        if (o == this) return true;
	        if ((o instanceof DTJSubtrajIntersectPart)) {
		        
	        	DTJSubtrajIntersectPart pair = (DTJSubtrajIntersectPart) o;
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
	    
	public int compareTo(DTJSubtrajIntersectPart objKeyPair) {

		int result = Integer.compare(obj_id, objKeyPair.obj_id);
		
		if (result == 0){
			 
			result = Integer.compare(traj_id, objKeyPair.traj_id);
		}
		
		if (result == 0){
 
			result = Integer.compare(subtraj_id, objKeyPair.subtraj_id);
		}
		
		if (result == 0){
			 
			result = Integer.compare(t_min, objKeyPair.t_min);
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
	public int gett_min() {
		return t_min;
	}
	public void sett_min(int t_min) {
		this.t_min = t_min;
	}	

	
	public DTJSubtraj toDTJSubtraj() {
		
		return new DTJSubtraj(this.obj_id, this.traj_id, this.subtraj_id);
		
	}
}