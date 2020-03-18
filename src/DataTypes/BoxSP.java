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


public class BoxSP implements Writable, WritableComparable<BoxSP> {
	public PointSP lp;
	public PointSP hp;


	public BoxSP() {
		this.setBoxSP(new PointSP(), new PointSP());
	}
	
	public BoxSP(PointSP lp, PointSP hp) {
		this.setBoxSP(lp, hp);
	}
	
	public BoxSP(BoxSP b){
		 this(b.lp, b.hp);
	}
	
	public void setBoxSP(PointSP lp, PointSP hp) {
		this.lp = lp;
		this.hp = hp;
	}
	
	public void setBoxSP_lp(PointSP lp) {
		this.lp = lp;
	}

	public PointSP getBoxSP_lp() {
		return lp;
	}
	
	public void setBoxSP_hp(PointSP hp) {
		this.hp = hp;
	}

	public PointSP getBoxSP_hp() {
		return hp;
	}
	
	@Override
	public String toString() {
		return (new StringBuilder().append(lp).append(",").append(hp)).toString();
	}
	
	public void readFields(DataInput dataInput) throws IOException {
		List<Integer> IntList= new ArrayList<Integer>();
		for(int i = 0; i < 6; i++){
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

		if (IntList.size() == 4){
			lp = new PointSP(IntList.get(0), IntList.get(1));
			hp = new PointSP(IntList.get(2), IntList.get(3));
		} else if (IntList.size() == 6){
			lp = new PointSP(IntList.get(0), IntList.get(1), IntList.get(2));
			hp = new PointSP(IntList.get(3), IntList.get(4), IntList.get(5));
		}		
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeVInt(dataOutput, lp.x);
		WritableUtils.writeVInt(dataOutput, lp.y);
		if (lp.n_of_dims() == 3){WritableUtils.writeVInt(dataOutput, lp.z);}
		WritableUtils.writeVInt(dataOutput, hp.x);
		WritableUtils.writeVInt(dataOutput, hp.y);
		if (hp.n_of_dims() == 3){WritableUtils.writeVInt(dataOutput, hp.z);}
	}
	

	
	

	public int compareTo(BoxSP o) {
		
	    // Sort by tmin, then lx and then ly
	    if (this.lp.x < o.lp.x)
	      return -1;
	    if (this.lp.x > o.lp.x)
	      return 1;
	    if (this.lp.y < o.lp.y)
	      return -1;
	    if (this.lp.y > o.lp.y)
	      return 1;
	    
	    if (this.lp.n_of_dims() == 3){
		    if (this.lp.z < o.lp.z)
			  return -1;
			if (this.lp.z > o.lp.z)
			  return 1;
	    }

	    // Sort by tmax, then hx and then hy
	    if (this.hp.x < o.hp.x)
	      return -1;
	    if (this.hp.x > o.hp.x)
	      return 1;
	    if (this.hp.y < o.hp.y)
	      return -1;
	    if (this.hp.y > o.hp.y)
	      return 1;
	    
	    if (this.lp.n_of_dims() == 3){
		    if (this.hp.z < o.hp.z)
			  return -1;
			if (this.hp.z > o.hp.z)
			  return 1;
	    }
		
	    return 0;

	}
	
	public BoxSP BoxSPExpand(int epsilon_sp){
		if (this.lp.n_of_dims() == 2){
			
			BoxSP m = new BoxSP(new PointSP(this.lp.x - epsilon_sp, this.lp.y - epsilon_sp), new PointSP(this.hp.x + epsilon_sp, this.hp.y + epsilon_sp));
			return m;
			
		} else {
			
			BoxSP m = new BoxSP(new PointSP(this.lp.x - epsilon_sp, this.lp.y - epsilon_sp, this.lp.z - epsilon_sp), new PointSP(this.hp.x + epsilon_sp, this.hp.y + epsilon_sp, this.hp.z + epsilon_sp));
			return m;

		}
	}

	public boolean BoxSPIntersects(BoxSP o) {
		if (this.lp.n_of_dims() == 2 || o.lp.n_of_dims() == 2){
			if ((((this.lp.x <= o.hp.x && o.hp.x <= this.hp.x) || (o.lp.x <= this.hp.x && this.hp.x <= o.hp.x)) && ((this.lp.y <= o.hp.y && o.hp.y <= this.hp.y) || (o.lp.y <= this.hp.y && this.hp.y <= o.hp.y)))){

				return true;
				
			} else return false;				

		} else {
			if ((((this.lp.x <= o.hp.x && o.hp.x <= this.hp.x) || (o.lp.x <= this.hp.x && this.hp.x <= o.hp.x)) && ((this.lp.y <= o.hp.y && o.hp.y <= this.hp.y) || (o.lp.y <= this.hp.y && this.hp.y <= o.hp.y)) && ((this.lp.z <= o.hp.z && o.hp.z <= this.hp.z) || (o.lp.z <= this.hp.z && this.hp.z <= o.hp.z)))){

				return true;
				
			} else return false;				
		}
	 	
	}
	
	public boolean BoxSPIntersects(PointSP o) {
		if (this.lp.n_of_dims() == 2 || o.n_of_dims() == 2){
			if ((((this.lp.x <= o.x && o.x < this.hp.x) || (o.x <= this.hp.x && this.hp.x < o.x)) && ((this.lp.y <= o.y && o.y < this.hp.y) || (o.y < this.hp.y && this.hp.y < o.y)))){

				return true;
				
			} else return false;				
			
		} else {
			if ((((this.lp.x <= o.x && o.x < this.hp.x) || (o.x <= this.hp.x && this.hp.x < o.x)) && ((this.lp.y <= o.y && o.y < this.hp.y) || (o.y < this.hp.y && this.hp.y < o.y)) && ((this.lp.z <= o.z && o.z < this.hp.z) || (o.z < this.hp.z && this.hp.z < o.z)))){

				return true;
				
			} else return false;				
		}
	 	
	}
	
	public BoxSP BoxSPIntersection(BoxSP o) {
		if (this.lp.n_of_dims() == 2 || o.lp.n_of_dims() == 2){
			int ref_point_lx = Math.max(this.lp.x, o.lp.x);
			int ref_point_ly = Math.max(this.lp.y, o.lp.y);
			int ref_point_hx = Math.min(this.hp.x, o.hp.x);
			int ref_point_hy = Math.min(this.hp.y, o.hp.y);
			
			BoxSP i = new BoxSP(new PointSP(ref_point_lx, ref_point_ly), new PointSP(ref_point_hx, ref_point_hy));
			return i;

		} else {
			int ref_point_lx = Math.max(this.lp.x, o.lp.x);
			int ref_point_ly = Math.max(this.lp.y, o.lp.y);
			int ref_point_lz = Math.max(this.lp.z, o.lp.z);

			int ref_point_hx = Math.min(this.hp.x, o.hp.x);
			int ref_point_hy = Math.min(this.hp.y, o.hp.y);
			int ref_point_hz = Math.min(this.hp.z, o.hp.z);

			BoxSP i = new BoxSP(new PointSP(ref_point_lx, ref_point_ly, ref_point_lz), new PointSP(ref_point_hx, ref_point_hy, ref_point_hz));
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
	
	public boolean BoxSPContains(PointSP p) {
		if (this.lp.n_of_dims() == 2 || p.n_of_dims() == 2){
			if (p.x >= this.lp.x && p.x <= this.hp.x && p.y >= this.lp.y && p.y <= this.hp.y){
				
				return true;

			}
			
			return false;
			
		} else {
			if (p.x >= this.lp.x && p.x <= this.hp.x && p.y >= this.lp.y && p.y <= this.hp.y && p.z >= this.lp.z && p.z <= this.hp.z){
				
				return true;

			}
			
			return false;
		}
	 	
	}
	
	public boolean BoxSPContains_inc_excl(PointSP p) {
		if (this.lp.n_of_dims() == 2 || p.n_of_dims() == 2){
			if (p.x >= this.lp.x && p.x < this.hp.x && p.y >= this.lp.y && p.y < this.hp.y){
				
				return true;

			}
			
			return false;
			
		} else {
			if (p.x >= this.lp.x && p.x < this.hp.x && p.y >= this.lp.y && p.y < this.hp.y && p.z >= this.lp.z && p.z < this.hp.z){
				
				return true;

			}
			
			return false;
		}
	 	
	}
	
	public double BoxSPDiameter() {
		
		return this.lp.DistanceSP(this.hp);
	 	
	}
	

}
