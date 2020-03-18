package DataTypes;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;



public class OctTreeNode implements Writable, WritableComparable<QuadTreeNode> {
	public int ID;
	public BoxSP cell;
	public int q1ID;
	public int q2ID;
	public int q3ID;
	public int q4ID;
	public int q5ID;
	public int q6ID;
	public int q7ID;
	public int q8ID;

	
	public OctTreeNode(OctTreeNode b){
		 this(b.ID, b.cell, b.q1ID, b.q2ID, b.q3ID, b.q4ID, b.q5ID, b.q6ID, b.q7ID, b.q8ID);
	}
	
	public OctTreeNode(int ID, BoxSP cell, int q1ID, int q2ID, int q3ID, int q4ID, int q5ID, int q6ID, int q7ID, int q8ID) {
		this.ID = ID;
		this.cell = cell;
		this.q1ID = q1ID;
		this.q2ID = q2ID;
		this.q3ID = q3ID;
		this.q4ID = q4ID;
		this.q5ID = q5ID;
		this.q6ID = q6ID;
		this.q7ID = q7ID;
		this.q8ID = q8ID;


	}
	
	public OctTreeNode(int ID, BoxSP cell, int q1ID, int q2ID, int q3ID, int q4ID) {
		this.ID = ID;
		this.cell = cell;
		this.q1ID = q1ID;
		this.q2ID = q2ID;
		this.q3ID = q3ID;
		this.q4ID = q4ID;
		this.q5ID = -1;
		this.q6ID = -1;
		this.q7ID = -1;
		this.q8ID = -1;

	}
	
	public OctTreeNode() {

	}
	
	
	public int n_of_args() {
		if(this.q5ID == -1 && this.q6ID == -1 && this.q7ID == -1 && this.q8ID == -1){
			return 6;
		} else {
			return 10;
		}
	}

	@Override
	public String toString() {
		if (this.n_of_args() == 6){
			return (new StringBuilder().append(ID).append(",").append(cell).append(",").append(q1ID).append(",").append(q2ID).append(",").append(q3ID).append(",").append(q4ID)).toString();

		} else {
			return (new StringBuilder().append(ID).append(",").append(cell).append(",").append(q1ID).append(",").append(q2ID).append(",").append(q3ID).append(",").append(q4ID).append(",").append(q5ID).append(",").append(q6ID).append(",").append(q7ID).append(",").append(q8ID)).toString();
		}
	}
	
	public void readFields(DataInput dataInput) throws IOException {
		List<Integer> IntList= new ArrayList<Integer>();
		for(int i = 0; i < 15; i++){
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

		if (IntList.size() == 9){
			ID = IntList.get(0);
			cell = new BoxSP(new PointSP(IntList.get(1), IntList.get(2)), new PointSP(IntList.get(3), IntList.get(4)));
			q1ID = IntList.get(5);
			q2ID = IntList.get(6);
			q3ID = IntList.get(7);
			q4ID = IntList.get(8);
		} else if (IntList.size() == 15){
			ID = IntList.get(0);
			cell = new BoxSP(new PointSP(IntList.get(1), IntList.get(2), IntList.get(3)), new PointSP(IntList.get(4), IntList.get(5), IntList.get(6)));
			q1ID = IntList.get(7);
			q2ID = IntList.get(8);
			q3ID = IntList.get(9);
			q4ID = IntList.get(10);
			q5ID = IntList.get(11);
			q6ID = IntList.get(12);
			q7ID = IntList.get(13);
			q8ID = IntList.get(14);
		}
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeVInt(dataOutput, ID);
		WritableUtils.writeVInt(dataOutput, cell.hp.x);
		WritableUtils.writeVInt(dataOutput, cell.hp.y);
		if (cell.hp.n_of_dims() == 3){WritableUtils.writeVInt(dataOutput, cell.hp.z);}
		WritableUtils.writeVInt(dataOutput, cell.lp.x);
		WritableUtils.writeVInt(dataOutput, cell.lp.y);
		if (cell.hp.n_of_dims() == 3){WritableUtils.writeVInt(dataOutput, cell.lp.z);}
		WritableUtils.writeVInt(dataOutput, q1ID);
		WritableUtils.writeVInt(dataOutput, q2ID);
		WritableUtils.writeVInt(dataOutput, q3ID);
		WritableUtils.writeVInt(dataOutput, q4ID);
		if (cell.hp.n_of_dims() == 3){
			WritableUtils.writeVInt(dataOutput, q5ID);
			WritableUtils.writeVInt(dataOutput, q6ID);
			WritableUtils.writeVInt(dataOutput, q7ID);
			WritableUtils.writeVInt(dataOutput, q8ID);
		}

	}
	
	public void setOctTreeNode(int ID, BoxSP cell, int q1ID, int q2ID, int q3ID, int q4ID, int q5ID, int q6ID, int q7ID, int q8ID) {
		this.ID = ID;
		this.cell = cell;
		this.q1ID = q1ID;
		this.q2ID = q2ID;
		this.q3ID = q3ID;
		this.q4ID = q4ID;
		this.q5ID = q5ID;
		this.q6ID = q6ID;
		this.q7ID = q7ID;
		this.q8ID = q8ID;
	}
	
	public void setOctTreeNode(int ID, BoxSP cell, int q1ID, int q2ID, int q3ID, int q4ID) {
		this.ID = ID;
		this.cell = cell;
		this.q1ID = q1ID;
		this.q2ID = q2ID;
		this.q3ID = q3ID;
		this.q4ID = q4ID;
		this.q5ID = -1;
		this.q6ID = -1;
		this.q7ID = -1;
		this.q8ID = -1;
	}
	
	public int getID() {
		return ID;
	}
	public void setID(int ID) {
		this.ID = ID;
	}
	public BoxSP getCell() {
		return cell;
	}
	public void setMBB(BoxSP cell) {
		this.cell = cell;
	}
	public int getq1ID() {
		return q1ID;
	}
	public void setq1ID(int q1ID) {
		this.q1ID = q1ID;
	}
	public int getq2ID() {
		return q2ID;
	}
	public void setq2ID(int q2ID) {
		this.q2ID = q2ID;
	}
	public int getq3ID() {
		return q3ID;
	}
	public void setq3ID(int q3ID) {
		this.q3ID = q3ID;
	}
	public int getq4ID() {
		return q4ID;
	}
	public void setq4ID(int q4ID) {
		this.q4ID = q4ID;
	}
	public int getq5ID() {
		return q5ID;
	}
	public void setq5ID(int q5ID) {
		this.q5ID = q5ID;
	}
	public int getq6ID() {
		return q6ID;
	}
	public void setq6ID(int q6ID) {
		this.q6ID = q6ID;
	}
	public int getq7ID() {
		return q7ID;
	}
	public void setq7ID(int q7ID) {
		this.q7ID = q7ID;
	}
	public int getq8ID() {
		return q8ID;
	}
	public void setq8ID(int q8ID) {
		this.q8ID = q8ID;
	}

	
	public int compareTo(QuadTreeNode o) {
		
	    if (this.ID < o.ID)
	      return -1;
	    if (this.ID > o.ID)
	      return 1;
	    return 0;
	
	}
	
}
