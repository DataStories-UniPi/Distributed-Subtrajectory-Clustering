package CustomWritables;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class DTJobjPair implements Writable, WritableComparable<DTJobjPair> {

	public int r_obj_id;
	public int s_obj_id;

	public DTJobjPair() {
		
	}

	public DTJobjPair(int r_obj_id, int s_obj_id) {
		this.r_obj_id = r_obj_id;
		this.s_obj_id = s_obj_id;
	}

	@Override
	public String toString() {

		return (new StringBuilder().append(r_obj_id).append(",").append(s_obj_id).toString());
	}

	public void readFields(DataInput dataInput) throws IOException {
		
		r_obj_id = WritableUtils.readVInt(dataInput);
		s_obj_id = WritableUtils.readVInt(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		
		WritableUtils.writeVInt(dataOutput, r_obj_id);
		WritableUtils.writeVInt(dataOutput, s_obj_id);
	}
	
	 @Override
	    public boolean equals(Object o) {

	        if (o == this) return true;
	        if (!(o instanceof DTJobjPair)) {
	            return false;
	        }

	        DTJobjPair pair = (DTJobjPair) o;

	        return pair.r_obj_id == r_obj_id && pair.s_obj_id == s_obj_id;
	    }

	    //Idea from effective Java : Item 9
	    @Override
	    public int hashCode() {
	        int result = 17;
	        result = 31 * result + r_obj_id;
	        result = 31 * result + s_obj_id;
	        return result;
	    }
	    
	/*@Override
    public boolean equals(Object object)
    {
        boolean eq = false;

        if (object != null && object instanceof DTJobjPair)
        {

        	eq = (this.r_obj_id == ((DTJobjPair) object).r_obj_id && this.s_obj_id == ((DTJobjPair) object).s_obj_id);

        }

        return eq;
    }
	
	@Override 
    public int hashCode()
    {
        int hash = 17;
        hash = hash * 31 + r_obj_id;
        hash = hash * 31 + s_obj_id;
        return hash;
    }*/
	
	public int compareTo(DTJobjPair objKeyPair) {

		int result = Integer.compare(r_obj_id, objKeyPair.r_obj_id);
		
		if (result == 0){
 
			result =Integer.compare(s_obj_id, objKeyPair.s_obj_id);
		}
		return result;
	}

	public int getr_obj_id() {
		return r_obj_id;
	}
	public void setr_obj_id(int r_obj_id) {
		this.r_obj_id = r_obj_id;
	}

	public int gets_obj_id() {
		return s_obj_id;
	}
	public void sets_obj_id(int s_obj_id) {
		this.s_obj_id = s_obj_id;
	}

}