package DataTypes;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;



public class QuadTreeNode implements Writable, WritableComparable<QuadTreeNode> {
	public int ID;
	public BoxSP cell;
	//public int level;
	public int q1ID;
	public int q2ID;
	public int q3ID;
	public int q4ID;

	
	public QuadTreeNode(QuadTreeNode b){
		 this(b.ID, b.cell, /*b.level,*/ b.q1ID, b.q2ID, b.q3ID, b.q4ID);
	}
	
	public QuadTreeNode(int ID, BoxSP cell, /*int level,*/ int q1ID, int q2ID, int q3ID, int q4ID) {
		this.ID = ID;
		this.cell = cell;
		//this.level = level;
		this.q1ID = q1ID;
		this.q2ID = q2ID;
		this.q3ID = q3ID;
		this.q4ID = q4ID;


	}
	
	public QuadTreeNode() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public String toString() {

		return (new StringBuilder().append(ID).append(",").append(cell)/*.append(",").append(level)*/.append(",").append(q1ID).append(",").append(q2ID).append(",").append(q3ID).append(",").append(q4ID)).toString();
	}
	
	public void readFields(DataInput dataInput) throws IOException {
		ID = WritableUtils.readVInt(dataInput);
		cell.readFields(dataInput);
		//level = WritableUtils.readVInt(dataInput);
		q1ID = WritableUtils.readVInt(dataInput);
		q2ID = WritableUtils.readVInt(dataInput);
		q3ID = WritableUtils.readVInt(dataInput);
		q4ID = WritableUtils.readVInt(dataInput);


	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeVInt(dataOutput, ID);
		cell.write(dataOutput);
		//WritableUtils.writeVInt(dataOutput, level);
		WritableUtils.writeVInt(dataOutput, q1ID);
		WritableUtils.writeVInt(dataOutput, q2ID);
		WritableUtils.writeVInt(dataOutput, q3ID);
		WritableUtils.writeVInt(dataOutput, q4ID);


	}
	
	public void setQuadTreeNode(int ID, BoxSP cell, int level, int q1ID, int q2ID, int q3ID, int q4ID) {
		this.ID = ID;
		this.cell = cell;
		//this.level = level;
		this.q1ID = q1ID;
		this.q2ID = q2ID;
		this.q3ID = q3ID;
		this.q4ID = q4ID;
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
	/*
	public int getLevel() {
	
		return level;
	}
	public void setLevel(int level) {
		this.level = level;
	}
	*/
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

	
	public int compareTo(QuadTreeNode o) {
		
	    // Sort by tmin, then lx and then ly
	    if (this.ID < o.ID)
	      return -1;
	    if (this.ID > o.ID)
	      return 1;
	    return 0;
	
	}
	
	 /*@Override
	    public int hashCode() {
	        return String.valueOf(ID).hashCode();
	    }*/
	

	
}
