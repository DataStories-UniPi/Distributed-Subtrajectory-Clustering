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


public class PointST implements Writable, WritableComparable<PointST> {
	public int t;
	public PointSP p;

	public PointST() {
	    this(0, new PointSP());
	  }
	
	public PointST(int t, PointSP p) {
		this.setPointST(t, p);
	}
	
	
	public PointST(PointST b){
		 this(b.t, b.p);
	}

	public void setPointST(int t, PointSP p) {
		this.t = t;
		this.p = p;
	}
	
	public int getPointST_t() {
		return t;
	}
	
	public int getPointST_x() {
		return p.x;
	}
	
	public int getPointST_y() {
		return p.y;
	}
	
	public int getPointST_z() {
		return p.z;
	}

	@Override
	public String toString() {
		
			
		return (new StringBuilder().append(t).append(",").append(p)).toString();

	}
	
	public void readFields(DataInput dataInput) throws IOException {
		List<Integer> IntList= new ArrayList<Integer>();
		for(int i = 0; i < 4; i++){
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
		t = IntList.get(0);
		if (IntList.size() == 3){
			p = new PointSP(IntList.get(1), IntList.get(2));
		} else if (IntList.size() == 4){
			p = new PointSP(IntList.get(1), IntList.get(2),  IntList.get(3));
		}
	}

	public void write(DataOutput dataOutput) throws IOException {

		WritableUtils.writeVInt(dataOutput, t);
		WritableUtils.writeVInt(dataOutput, p.x);
		WritableUtils.writeVInt(dataOutput, p.y);
		if (p.n_of_dims() == 3){WritableUtils.writeVInt(dataOutput, p.z);}
	
	}
	
	/*@Override
    public boolean equals(Object o) {

        if (o == this) return true;
        if (!(o instanceof PointST)) {
            return false;
        }

        PointST point = (PointST) o;

        return point.t == t && point.p.x == p.x && point.p.y == p.y;
    }

    //Idea from effective Java : Item 9
    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + t;
        result = 31 * result + p.x;
        result = 31 * result + p.y;
        return result;
    }*/
	
	public int compareTo(PointST o) {
		
	    // Sort by t, then x and then y
		int result = Integer.compare(t, o.t);
		
		if (result == 0){
 
			result =Integer.compare(p.x, o.p.x);
		}
		if (result == 0){
			 
			result =Integer.compare(p.y, o.p.y);
		}

		return result;
	}/*
	    if (this.t < o.t)
	      return -1;
	    if (this.t > o.t)
	      return 1;
	    if (this.p.x < o.p.x)
	      return -1;
	    if (this.p.x > o.p.x)
	      return 1;
	    if (this.p.y < o.p.y)
	      return -1;
	    if (this.p.y > o.p.y)
	      return 1;
	    
	    if (this.p.n_of_dims() == 3){
		    if (this.p.z < o.p.z)
			      return -1;
		    if (this.p.z > o.p.z)
		      return 1;
	    }

	    return 0;
	
	}*/
	
	
	public BoxST PointSTExpand(int epsilon_sp, int epsilon_t){
		if (this.p.n_of_dims() == 2){
			int te = getPointST_t() + epsilon_t;
			int hx2 = getPointST_x() + epsilon_sp;
			int hy2 = getPointST_y() + epsilon_sp;
			
			int ti = getPointST_t() - epsilon_t;
			int lx2 = getPointST_x() - epsilon_sp;
			int ly2 = getPointST_y() - epsilon_sp;
			
			PointST lp = new PointST(ti, new PointSP(lx2, ly2));
			
			PointST hp = new PointST(te , new PointSP(hx2, hy2));
			
			BoxST m = new BoxST(lp, hp);
			
			return m;
		} else {
			int te = getPointST_t() + epsilon_t;
			int hx2 = getPointST_x() + epsilon_sp;
			int hy2 = getPointST_y() + epsilon_sp;
			int hz2 = getPointST_z() + epsilon_sp;

			int ti = getPointST_t() - epsilon_t;
			int lx2 = getPointST_x() - epsilon_sp;
			int ly2 = getPointST_y() - epsilon_sp;
			int lz2 = getPointST_z() - epsilon_sp;

			PointST lp = new PointST(ti, new PointSP(lx2, ly2, lz2));
			
			PointST hp = new PointST(te, new PointSP(hx2, hy2, hz2));
			
			BoxST m = new BoxST(lp, hp);
			
			return m;
		}

	}
	
	public BoxSP getMBB(){
		
		BoxSP mbb = new BoxSP(this.p, this.p);
		return mbb;
}
	
	
	public PointSP toPointSP(){
		return this.p;
	}
	
}
