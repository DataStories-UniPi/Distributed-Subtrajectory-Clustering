package DataTypes;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;



public class PointSP implements Writable, WritableComparable<PointSP> {
	public int x;
	public int y;
	public int z;

	public PointSP() {
	    
	}
	
	public PointSP(int x, int y) {
		this.setPointSP(x, y);
	}
	
	public PointSP(int x, int y, int z) {
		this.setPointSP(x, y, z);
	}
	
	public PointSP(PointSP b){
		 this(b.x, b.y, b.z);
	}

	public void setPointSP(int x, int y) {
		this.x = x;
		this.y = y;
		this.z = Integer.MIN_VALUE;
	}
	
	public void setPointSP(int x, int y, int z) {
		this.x = x;
		this.y = y;
		this.z = z;

	}
	
	public void setPointSP_x(int x) {
		this.x = x;
	}
	
	public int getPointSP_x() {
		return x;
	}
	
	public void setPointSP_y(int y) {
		this.y = y;
	}

	public int getPointSP_y() {
		return y;
	}
	
	public void setPointSP_z(int z) {
		this.z = z;
	}

	public int getPointSP_z() {
		return z;
	}

	public int n_of_args() {
		if(this.z == Integer.MIN_VALUE){
			return 2;
		} else {
			return 3;
		}
	}	
	@Override
	public String toString() {
		
		if (this.n_of_args() == 2){
			
			return (new StringBuilder().append(x).append(",").append(y)).toString();

		} else {
			
			return (new StringBuilder().append(x).append(",").append(y).append(",").append(z)).toString();

		}
	}
	
	public int n_of_dims() {
		StringTokenizer linetokenizer = new StringTokenizer(this.toString(), ",");
		return linetokenizer.countTokens();
	}
	
	public void readFields(DataInput dataInput) throws IOException {
		List<Integer> IntList= new ArrayList<Integer>();
		for(int i = 0; i < 3; i++){
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
		
		x = IntList.get(0);
		y = IntList.get(1);
		if (IntList.size() == 3){z = IntList.get(2);}

	}

	public void write(DataOutput dataOutput) throws IOException {

		WritableUtils.writeVInt(dataOutput, x);
		WritableUtils.writeVInt(dataOutput, y);
		if (this.n_of_dims() == 3){WritableUtils.writeVInt(dataOutput, z);}

	}
	
	public int compareTo(PointSP o) {
		
	    // Sort by t, then x and then y
	    if (this.x < o.x)
	      return -1;
	    if (this.x > o.x)
	      return 1;
	    if (this.y < o.y)
	      return -1;
	    if (this.y > o.y)
	      return 1;
	    
	    if (this.n_of_dims() == 3){
		    if (this.z < o.z)
			      return -1;
		    if (this.z > o.z)
		      return 1;
	    }

	    return 0;
	
	}
	
	
	public BoxSP PointSPExpand(int epsilon_sp){
		if (this.n_of_dims() == 2){
			int hx2 = getPointSP_x() + epsilon_sp;
			int hy2 = getPointSP_y() + epsilon_sp;
			
			int lx2 = getPointSP_x() - epsilon_sp;
			int ly2 = getPointSP_y() - epsilon_sp;
			
			PointSP lp = new PointSP(lx2, ly2);
			
			PointSP hp = new PointSP(hx2, hy2);
			
			BoxSP m = new BoxSP(lp, hp);
			
			return m;
		} else {
			int hx2 = getPointSP_x() + epsilon_sp;
			int hy2 = getPointSP_y() + epsilon_sp;
			int hz2 = getPointSP_z() + epsilon_sp;

			int lx2 = getPointSP_x() - epsilon_sp;
			int ly2 = getPointSP_y() - epsilon_sp;
			int lz2 = getPointSP_z() - epsilon_sp;

			PointSP lp = new PointSP(lx2, ly2, lz2);
			
			PointSP hp = new PointSP(hx2, hy2, hz2);
			
			BoxSP m = new BoxSP(lp, hp);
			
			return m;
		}

	}
	
	public BoxSP getMBB(){
		if (this.n_of_dims() == 2){
			PointSP p = new PointSP(this.x, this.y);
			
			BoxSP mbb = new BoxSP(p,p);
			
			return mbb;
		} else {
			PointSP p = new PointSP(this.x, this.y, this.z);

			BoxSP mbb = new BoxSP(p, p);
			return mbb;
		}

	}
	
	
	public double DistanceSP(PointSP p){
		if (this.n_of_dims() == 2){
			double dist = Math.sqrt(Math.pow((this.x-p.x), 2) + Math.pow((this.y - p.y),2));
			return dist;
		} else {
			double dist = Math.sqrt(Math.pow((this.x-p.x), 2) + Math.pow((this.y - p.y),2) + Math.pow((this.z - p.z),2));
			return dist;
		}

	}
	

	public double SimilaritySP(PointSP p, double epsilon_sp){

		double sim = 1 - (double)this.DistanceSP(p)/(epsilon_sp);

		return sim;
	}
	

}
