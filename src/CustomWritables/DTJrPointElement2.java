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


public class DTJrPointElement2 implements Writable,	WritableComparable<DTJrPointElement2> {

	public int obj_id;
	public int traj_id;
	public PointST point;
	public int epsilon_sp;

	public DTJrPointElement2(int obj_id, int traj_id, PointST point) {
		this.obj_id = obj_id;
		this.traj_id = traj_id;
		this.point = point;
		this.epsilon_sp = 0;
	}
	
	public DTJrPointElement2(int obj_id, int traj_id, PointST point, int epsilon_sp) {
		this.obj_id = obj_id;
		this.traj_id = traj_id;
		this.point = point;
		this.epsilon_sp = epsilon_sp;
	}
	
	public DTJrPointElement2() {
		
	}


	@Override
	public String toString() {

		return (new StringBuilder().append(obj_id).append(",").append(traj_id).append(",").append(point)/*.append(", ").append(epsilon_sp)*/.toString());
	}

	public void readFields(DataInput dataInput) throws IOException {
		List<Integer> IntList= new ArrayList<Integer>();
		for(int i = 0; i < 7; i++){
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

		if (IntList.size() == 6){
			obj_id = IntList.get(0);
			traj_id = IntList.get(1);
			point = new PointST(IntList.get(2), new PointSP(IntList.get(3), IntList.get(4)));
			epsilon_sp = IntList.get(5);
		} else if (IntList.size() == 7){
			obj_id = IntList.get(0);
			traj_id = IntList.get(1);
			point = new PointST(IntList.get(2), new PointSP(IntList.get(3), IntList.get(4), IntList.get(5)));
			epsilon_sp = IntList.get(6);
		}
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeVInt(dataOutput, obj_id);
		WritableUtils.writeVInt(dataOutput, traj_id);
		WritableUtils.writeVInt(dataOutput, point.t);
		WritableUtils.writeVInt(dataOutput, point.p.x);
		WritableUtils.writeVInt(dataOutput, point.p.y);
		if (point.p.n_of_dims() == 3){WritableUtils.writeVInt(dataOutput, point.p.z);}
		WritableUtils.writeVInt(dataOutput, epsilon_sp);
	}

	public int compareTo(DTJrPointElement2 objKeyPair) {
		
		int result = Integer.compare(obj_id, objKeyPair.obj_id);
		
		if (result == 0){
			result =Long.compare(traj_id, objKeyPair.traj_id);
		}
		if (result == 0){
			result =Integer.compare(point.t, objKeyPair.point.t);
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
	public PointST getPoint() {
		return point;
	}
	public void setPoint(PointST point) {
		this.point = point;
	}
	public int getepsilon_sp() {
		return epsilon_sp;
	}
	public void setepsilon_sp(int epsilon_sp) {
		this.epsilon_sp = epsilon_sp;
	}
}