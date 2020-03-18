package CustomWritables;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class DTJSubtrajValue implements Writable, WritableComparable<DTJSubtrajValue> {

	public int mint;
	public int maxt;
	public int n_of_points;
	public double sum_voting;
	public DTJSubtraj repr_subtraj;
	public double clust_sim;

	public DTJSubtrajValue() {
		
	}

	public DTJSubtrajValue(int mint, int maxt, int n_of_points, double sum_voting) {
		this.mint = mint;
		this.maxt = maxt;
		this.n_of_points = n_of_points;
		this.sum_voting = sum_voting;
		this.repr_subtraj = new DTJSubtraj(-1,-1,-1);
		this.clust_sim = -1;

	}
	
	public DTJSubtrajValue(int mint, int maxt, int n_of_points, double sum_voting, DTJSubtraj repr_subtraj, double clust_sim) {
		this.mint = mint;
		this.maxt = maxt;
		this.n_of_points = n_of_points;
		this.sum_voting = sum_voting;
		this.repr_subtraj = repr_subtraj;
		this.clust_sim = clust_sim;

	}
	
	@Override
	public String toString() {
			
		return (new StringBuilder().append(mint).append(",").append(maxt).append(",").append(n_of_points).append(",").append(sum_voting).append(",").append(repr_subtraj).append(",").append(clust_sim).toString());

	}
	

	public void readFields(DataInput dataInput) throws IOException {
		DoubleWritable sv_writable = new DoubleWritable();
		DoubleWritable cs_writable = new DoubleWritable();

		mint = WritableUtils.readVInt(dataInput);
		maxt = WritableUtils.readVInt(dataInput);
		n_of_points = WritableUtils.readVInt(dataInput);
		sv_writable.readFields(dataInput);
		sum_voting = sv_writable.get();
		repr_subtraj.obj_id = WritableUtils.readVInt(dataInput);
		repr_subtraj.traj_id = WritableUtils.readVInt(dataInput);
		repr_subtraj.subtraj_id = WritableUtils.readVInt(dataInput);
		cs_writable.readFields(dataInput);
		clust_sim = cs_writable.get();


	}

	public void write(DataOutput dataOutput) throws IOException {
		DoubleWritable sv_writable = new DoubleWritable();
		DoubleWritable cs_writable = new DoubleWritable();

		WritableUtils.writeVInt(dataOutput, mint);
		WritableUtils.writeVInt(dataOutput, maxt);
		WritableUtils.writeVInt(dataOutput, n_of_points);
		sv_writable.set(sum_voting);
		sv_writable.write(dataOutput);
		WritableUtils.writeVInt(dataOutput, repr_subtraj.obj_id);
		WritableUtils.writeVInt(dataOutput, repr_subtraj.traj_id);
		WritableUtils.writeVInt(dataOutput, repr_subtraj.subtraj_id);
		cs_writable.set(clust_sim);
		cs_writable.write(dataOutput);

		

	}
	
	 @Override
	    public boolean equals(Object o) {

	        if (o == this) return true;
	        if (!(o instanceof DTJSubtrajValue)) {
	            return false;
	        }

	        DTJSubtrajValue pair = (DTJSubtrajValue) o;

	        return pair.mint == mint && pair.maxt == maxt && pair.n_of_points == n_of_points;
	    }

	    //Idea from effective Java : Item 9
	    @Override
	    public int hashCode() {
	        int result = 17;
	        result = 31 * result + mint;
	        result = 31 * result + maxt;
	        result = 31 * result + n_of_points;
	        return result;
	    }
	    
	public int compareTo(DTJSubtrajValue objKeyPair) {


		int result =Integer.compare(mint, objKeyPair.mint);
		if (result == 0){
			 
			result =Integer.compare(maxt, objKeyPair.maxt);
		}
		if (result == 0){
			 
			result =Integer.compare(n_of_points, objKeyPair.n_of_points);
		}

		return result;
	}

	public int getmint() {
		return mint;
	}
	public void setmint(int mint) {
		this.mint = mint;
	}
	public int getmaxt() {
		return maxt;
	}
	public void setmaxt(int maxt) {
		this.maxt = maxt;
	}
	public int getn_of_points() {
		return  n_of_points;
	}
	public void setn_of_points(int n_of_points) {
		this.n_of_points = n_of_points;
	}
	public double getsum_voting() {
		return sum_voting;
	}
	public void setsum_voting(double sum_voting) {
		this.sum_voting = sum_voting;
	}
	public DTJSubtraj getrepr_subtraj() {
		return repr_subtraj;
	}
	public void setrepr_subtraj(DTJSubtraj repr_subtraj) {
		this.repr_subtraj = repr_subtraj;
	}
	public double getclust_sim() {
		return clust_sim;
	}
	public void setclust_sim(double clust_sim) {
		this.clust_sim = clust_sim;
	}


}