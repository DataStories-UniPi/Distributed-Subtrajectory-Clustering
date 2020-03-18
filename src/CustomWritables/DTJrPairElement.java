package CustomWritables;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import DataTypes.PointSP;
import DataTypes.PointST;


public class DTJrPairElement implements Writable,	WritableComparable<DTJrPairElement> {

	public int r_obj_id;
	public int r_traj_id;
	public PointST r_p;
	public int s_obj_id;
	public int s_traj_id;
	public PointST s_p;

	public DTJrPairElement() {
		
	}

	public DTJrPairElement(int r_obj_id, int r_traj_id, PointST r_p, int s_obj_id, int s_traj_id, PointST s_p) {
		this.r_obj_id = r_obj_id;
		this.r_traj_id = r_traj_id;
		this.r_p = r_p;
		this.s_obj_id = s_obj_id;
		this.s_traj_id = s_traj_id;
		this.s_p = s_p;
	}

	@Override
	public String toString() {

		return (new StringBuilder().append(r_obj_id).append(",").append(r_traj_id).append(",").append(r_p).append(",").append(s_obj_id).append(",").append(s_traj_id).append(",").append(s_p).toString());
	}

	public void readFields(DataInput dataInput) throws IOException {
		List<Integer> IntList= new ArrayList<Integer>();
		for(int i = 0; i < 12; i++){
			try{
				IntList.add(WritableUtils.readVInt(dataInput));
			}
            catch (EOFException ex1) {
            	break;
			}
            catch (IOException ex2) {
                System.err.println("An IOException was caught: " + ex2.getMessage());
                ex2.printStackTrace();
            }
		}

		if (IntList.size() == 10){
			r_obj_id = IntList.get(0);
			r_traj_id = IntList.get(1);
			r_p = new PointST(IntList.get(2), new PointSP(IntList.get(3), IntList.get(4)));
			s_obj_id = IntList.get(5);
			s_traj_id = IntList.get(6);
			s_p = new PointST(IntList.get(7), new PointSP(IntList.get(8), IntList.get(9)));
		} else if (IntList.size() == 12){
			r_obj_id = IntList.get(0);
			r_traj_id = IntList.get(1);
			r_p = new PointST(IntList.get(2), new PointSP(IntList.get(3), IntList.get(4), IntList.get(5)));
			s_obj_id = IntList.get(6);
			s_traj_id = IntList.get(7);
			s_p = new PointST(IntList.get(8), new PointSP(IntList.get(9), IntList.get(10), IntList.get(11)));

		}

	}

	public void write(DataOutput dataOutput) throws IOException {
		
		WritableUtils.writeVInt(dataOutput, r_obj_id);
		WritableUtils.writeVInt(dataOutput, r_traj_id);
		WritableUtils.writeVInt(dataOutput, r_p.t);
		WritableUtils.writeVInt(dataOutput, r_p.p.x);
		WritableUtils.writeVInt(dataOutput, r_p.p.y);
		if (r_p.p.n_of_dims() == 3){WritableUtils.writeVInt(dataOutput, r_p.p.z);}
		WritableUtils.writeVInt(dataOutput, s_obj_id);
		WritableUtils.writeVInt(dataOutput, s_traj_id);
		WritableUtils.writeVInt(dataOutput, s_p.t);
		WritableUtils.writeVInt(dataOutput, s_p.p.x);
		WritableUtils.writeVInt(dataOutput, s_p.p.y);
		if (s_p.p.n_of_dims() == 3){WritableUtils.writeVInt(dataOutput, s_p.p.z);}
	}
	
	
    public boolean equals(DTJrPairElement object)
    {
        boolean eq = false;

        if (object != null && object instanceof DTJrPairElement)
        {
        	eq = (this.r_obj_id == ((DTJrPairElement) object).r_obj_id && this.r_traj_id == ((DTJrPairElement) object).r_traj_id && this.r_p.t == ((DTJrPairElement) object).r_p.t && this.s_obj_id == ((DTJrPairElement) object).s_obj_id && this.s_traj_id == ((DTJrPairElement) object).s_traj_id && this.s_p.t == ((DTJrPairElement) object).s_p.t);
        }

        return eq;
    }
	
	public int compareTo(DTJrPairElement objKeyPair) {

		int result = Integer.compare(r_obj_id, objKeyPair.r_obj_id);
		
		if (result == 0){
			result =Long.compare(r_traj_id, objKeyPair.r_traj_id);
		}
		if (result == 0){
			result =Integer.compare(r_p.t, objKeyPair.r_p.t);
		}
		if (result == 0){
 
			result =Integer.compare(s_obj_id, objKeyPair.s_obj_id);
		}
		if (result == 0){
			result =Long.compare(s_traj_id, objKeyPair.s_traj_id);
		}
		if (result == 0){
			result =Integer.compare(s_p.t, objKeyPair.s_p.t);
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
	public PointST getr_point() {
		return r_p;
	}
	public void setr_point(PointST r_p) {
		this.r_p = r_p;
	}
	public int getr_x() {
		return r_p.p.x;
	}
	public void setr_x(int r_x) {
		this.r_p.p.x = r_x;
	}
	public int getr_y() {
		return r_p.p.y;
	}
	public void setr_y(int r_y) {
		this.r_p.p.y = r_y;
	}
	public int getr_z() {
		return r_p.p.z;
	}
	public void setr_z(int r_z) {
		this.r_p.p.z = r_z;
	}

	public int getr_t() {
		return r_p.t;
	}
	public void setr_t(int r_t) {
		this.r_p.t = r_t;
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
	public PointST gets_point() {
		return s_p;
	}
	public void sets_point(PointST s_p) {
		this.s_p = s_p;
	}
	public int gets_x() {
		return s_p.p.x;
	}
	public void sets_x(int s_x) {
		this.s_p.p.x = s_x;
	}
	public int gets_y() {
		return s_p.p.y;
	}
	public void sets_y(int s_y) {
		this.s_p.p.y = s_y;
	}
	public int gets_z() {
		return s_p.p.z;
	}
	public void sets_z(int s_z) {
		this.s_p.p.z = s_z;
	}

	public int gets_t() {
		return s_p.t;
	}
	public void sets_t(int s_t) {
		this.s_p.t = s_t;
	}
}