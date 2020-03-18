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


public class BoxST implements Writable, WritableComparable<BoxST> {
	public PointST lp;
	public PointST hp;

	public BoxST() {
		this.setBoxST(new PointST(), new PointST());
	}

	public BoxST(PointST lp, PointST hp) {
		this.setBoxST(lp, hp);
	}
	
	public BoxST(BoxST b){
		 this(b.lp, b.hp);
	}
	
	public void setBoxST(PointST lp, PointST hp) {
		this.lp = lp;
		this.hp = hp;
	}
	
	public PointST getBoxST_lp() {
		return lp;
	}
	
	public PointST getBoxST_hp() {
		return hp;
	}
	
	@Override
	public String toString() {
		return (new StringBuilder().append(lp).append(",").append(hp)).toString();
	}
	
	public void readFields(DataInput dataInput) throws IOException {
		List<Integer> IntList= new ArrayList<Integer>();
		for(int i = 0; i < 8; i++){
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
			lp = new PointST(IntList.get(0), new PointSP(IntList.get(1), IntList.get(2)));
			hp = new PointST(IntList.get(3), new PointSP(IntList.get(4), IntList.get(5)));
		} else if (IntList.size() == 8){
			lp = new PointST(IntList.get(0), new PointSP(IntList.get(1), IntList.get(2), IntList.get(3)));
			hp = new PointST(IntList.get(4), new PointSP(IntList.get(5), IntList.get(6), IntList.get(7)));
		}		

	}

	public void write(DataOutput dataOutput) throws IOException {

		WritableUtils.writeVInt(dataOutput, lp.t);
		WritableUtils.writeVInt(dataOutput, lp.p.x);
		WritableUtils.writeVInt(dataOutput, lp.p.y);
		if (lp.p.n_of_dims() == 3){WritableUtils.writeVInt(dataOutput, lp.p.z);}
		WritableUtils.writeVInt(dataOutput, hp.t);
		WritableUtils.writeVInt(dataOutput, hp.p.x);
		WritableUtils.writeVInt(dataOutput, hp.p.y);
		if (hp.p.n_of_dims() == 3){WritableUtils.writeVInt(dataOutput, hp.p.z);}
	}
	

	
	

	public int compareTo(BoxST o) {
		
	    // Sort by tmin, then lx and then ly
	    if (this.lp.t < o.lp.t)
	    	return -1;
		if (this.lp.t > o.lp.t)
			return 1;
	    if (this.lp.p.x < o.lp.p.x)
	      return -1;
	    if (this.lp.p.x > o.lp.p.x)
	      return 1;
	    if (this.lp.p.y < o.lp.p.y)
	      return -1;
	    if (this.lp.p.y > o.lp.p.y)
	      return 1;
	    
	    if (this.lp.p.n_of_dims() == 3){
		    if (this.lp.p.z < o.lp.p.z)
			  return -1;
			if (this.lp.p.z > o.lp.p.z)
			  return 1;
	    }

	    // Sort by tmax, then hx and then hy
	    if (this.hp.t < o.hp.t)
	    	return -1;
		if (this.hp.t > o.hp.t)
			return 1;
	    if (this.hp.p.x < o.hp.p.x)
	      return -1;
	    if (this.hp.p.x > o.hp.p.x)
	      return 1;
	    if (this.hp.p.y < o.hp.p.y)
	      return -1;
	    if (this.hp.p.y > o.hp.p.y)
	      return 1;
	    
	    if (this.lp.p.n_of_dims() == 3){
		    if (this.hp.p.z < o.hp.p.z)
			  return -1;
			if (this.hp.p.z > o.hp.p.z)
			  return 1;
	    }
		
	    return 0;

	}
	
	public BoxST BoxSPExpand(int epsilon_sp, int epsilon_t){
		if (this.lp.p.n_of_dims() == 2){
			
			BoxST m = new BoxST(new PointST(this.lp.t - epsilon_t, new PointSP(this.lp.p.x - epsilon_sp, this.lp.p.y - epsilon_sp)), new PointST(this.hp.t + epsilon_t, new PointSP(this.hp.p.x + epsilon_sp, this.hp.p.y + epsilon_sp)));
			return m;
			
		} else {
			
			BoxST m = new BoxST(new PointST(this.lp.t - epsilon_t, new PointSP(this.lp.p.x - epsilon_sp, this.lp.p.y - epsilon_sp, this.lp.p.z - epsilon_sp)), new PointST(this.hp.t + epsilon_t, new PointSP(this.hp.p.x + epsilon_sp, this.hp.p.y + epsilon_sp, this.hp.p.z + epsilon_sp)));
			return m;

		}
	}

	public boolean BoxSTIntersects(BoxST o) {
		if (this.lp.p.n_of_dims() == 2 || o.lp.p.n_of_dims() == 2){
			if ((((this.lp.p.x <= o.hp.p.x && o.hp.p.x <= this.hp.p.x) || (o.lp.p.x <= this.hp.p.x && this.hp.p.x <= o.hp.p.x)) && ((this.lp.p.y <= o.hp.p.y && o.hp.p.y <= this.hp.p.y) || (o.lp.p.y <= this.hp.p.y && this.hp.p.y <= o.hp.p.y))) && ((this.lp.t <= o.hp.t && o.hp.t <= this.hp.t) || (o.lp.t <= this.hp.t && this.hp.t <= o.hp.t))){

				return true;
				
			} else return false;				

		} else {
			if ((((this.lp.p.x <= o.hp.p.x && o.hp.p.x <= this.hp.p.x) || (o.lp.p.x <= this.hp.p.x && this.hp.p.x <= o.hp.p.x)) && ((this.lp.p.y <= o.hp.p.y && o.hp.p.y <= this.hp.p.y) || (o.lp.p.y <= this.hp.p.y && this.hp.p.y <= o.hp.p.y)) && ((this.lp.p.z <= o.hp.p.z && o.hp.p.z <= this.hp.p.z) || (o.lp.p.z <= this.hp.p.z && this.hp.p.z <= o.hp.p.z))) && ((this.lp.t <= o.hp.t && o.hp.t <= this.hp.t) || (o.lp.t <= this.hp.t && this.hp.t <= o.hp.t))){

				return true;
				
			} else return false;				
		}
	 	
	}
	
	public boolean BoxSTIntersects(PointST o) {
		if (this.lp.p.n_of_dims() == 2 || o.p.n_of_dims() == 2){
			if ((((this.lp.p.x <= o.p.x && o.p.x < this.hp.p.x) || (o.p.x <= this.hp.p.x && this.hp.p.x < o.p.x)) && ((this.lp.p.y <= o.p.y && o.p.y < this.hp.p.y) || (o.p.y < this.hp.p.y && this.hp.p.y < o.p.y)))&& ((this.lp.t <= o.t && o.t < this.hp.t) || (o.t < this.hp.t && this.hp.t < o.t))){

				return true;
				
			} else return false;				
			
		} else {
			if ((((this.lp.p.x <= o.p.x && o.p.x < this.hp.p.x) || (o.p.x <= this.hp.p.x && this.hp.p.x < o.p.x)) && ((this.lp.p.y <= o.p.y && o.p.y < this.hp.p.y) || (o.p.y < this.hp.p.y && this.hp.p.y < o.p.y)) && ((this.lp.p.z <= o.p.z && o.p.z < this.hp.p.z) || (o.p.z < this.hp.p.z && this.hp.p.z < o.p.z)))){

				return true;
				
			} else return false;				
		}
	 	
	}
	
	public BoxST BoxSTIntersection(BoxST o) {
		if (this.lp.p.n_of_dims() == 2 || o.lp.p.n_of_dims() == 2){
			int ref_point_tmin = Math.max(this.lp.t, o.lp.t);
			int ref_point_lx = Math.max(this.lp.p.x, o.lp.p.x);
			int ref_point_ly = Math.max(this.lp.p.y, o.lp.p.y);
			int ref_point_tmax = Math.min(this.hp.t, o.hp.t);
			int ref_point_hx = Math.min(this.hp.p.x, o.hp.p.x);
			int ref_point_hy = Math.min(this.hp.p.y, o.hp.p.y);
			
			BoxST i = new BoxST(new PointST(ref_point_tmin, new PointSP(ref_point_lx, ref_point_ly)), new PointST(ref_point_tmax, new PointSP(ref_point_hx, ref_point_hy)));
			return i;

		} else {
			int ref_point_tmin = Math.max(this.lp.t, o.lp.t);
			int ref_point_lx = Math.max(this.lp.p.x, o.lp.p.x);
			int ref_point_ly = Math.max(this.lp.p.y, o.lp.p.y);
			int ref_point_lz = Math.max(this.lp.p.z, o.lp.p.z);

			int ref_point_tmax = Math.min(this.hp.t, o.hp.t);
			int ref_point_hx = Math.min(this.hp.p.x, o.hp.p.x);
			int ref_point_hy = Math.min(this.hp.p.y, o.hp.p.y);
			int ref_point_hz = Math.min(this.hp.p.z, o.hp.p.z);

			BoxST i = new BoxST(new PointST(ref_point_tmin, new PointSP(ref_point_lx, ref_point_ly, ref_point_lz)), new PointST(ref_point_tmax, new PointSP(ref_point_hx, ref_point_hy, ref_point_hz)));
			return i;

		}
	 	
	}
	
	/*public PointST BoxSTCentroid() {
		
		int ref_point_x = (int)Math.ceil((double)this.lx + (double)(this.hx - this.lx)/2);
		int ref_point_y = (int)Math.ceil((double)this.ly + (double)(this.hy - this.ly)/2);
		int ref_point_t = (int)Math.ceil((double)this.tmin + (double)(this.tmax - this.tmin)/2);
		
		PointST p = new PointST(ref_point_t, ref_point_x, ref_point_y);
		return p;
	 	
	}*/
	
	public boolean BoxSTContains(PointST p) {
		if (this.lp.p.n_of_dims() == 2 || p.p.n_of_dims() == 2){
			if (p.p.x >= this.lp.p.x && p.p.x <= this.hp.p.x && p.p.y >= this.lp.p.y && p.p.y <= this.hp.p.y && p.t >= this.lp.t && p.t <= this.hp.t){
				
				return true;

			}
			
			return false;
			
		} else {
			
			if (p.p.x >= this.lp.p.x && p.p.x <= this.hp.p.x && p.p.y >= this.lp.p.y && p.p.y <= this.hp.p.y && p.p.z >= this.lp.p.z && p.p.z <= this.hp.p.z && p.t >= this.lp.t && p.t <= this.hp.t){

				
				return true;

			}
			
			return false;
		}
	 	
	}
	
	public boolean BoxSTContains_inc_excl(PointST p) {
		if (this.lp.p.n_of_dims() == 2 || p.p.n_of_dims() == 2){
			if (p.p.x >= this.lp.p.x && p.p.x < this.hp.p.x && p.p.y >= this.lp.p.y && p.p.y < this.hp.p.y && p.t >= this.lp.t && p.t < this.hp.t){
				
				return true;

			}
			
			return false;
			
		} else {
			if (p.p.x >= this.lp.p.x && p.p.x < this.hp.p.x && p.p.y >= this.lp.p.y && p.p.y < this.hp.p.y && p.p.z >= this.lp.p.z && p.p.z < this.hp.p.z && p.t >= this.lp.t && p.t < this.hp.t){

				return true;

			}
			
			return false;
		}
	 	
	}
	
	public BoxSP toBoxSP(){
		BoxSP b = new BoxSP(this.lp.toPointSP(), this.hp.toPointSP());
		return b;
	}
	
	public Period toPeriod(){
		Period b = new Period(this.lp.t, this.hp.t);
		return b;
	}
	
}
