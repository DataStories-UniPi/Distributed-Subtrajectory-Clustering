package CustomWritables;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class DTJSubtrajValueShort implements Writable, WritableComparable<DTJSubtrajValueShort> {

	public int mint;
	public int maxt;
	public int n_of_points;
	public double sum_voting;

	public DTJSubtrajValueShort() {
		
	}

	public DTJSubtrajValueShort(int mint, int maxt, int n_of_points, double sum_voting) {
		this.mint = mint;
		this.maxt = maxt;
		this.n_of_points = n_of_points;
		this.sum_voting = sum_voting;

	}
	
	public DTJSubtrajValueShort(int mint, int maxt, int n_of_points, double sum_voting, DTJSubtraj repr_subtraj, double clust_sim) {
		this.mint = mint;
		this.maxt = maxt;
		this.n_of_points = n_of_points;
		this.sum_voting = sum_voting;

	}
	
	@Override
	public String toString() {
			
		return (new StringBuilder().append(mint).append(",").append(maxt).append(",").append(n_of_points).append(",").append(sum_voting).toString());

	}
	

	public void readFields(DataInput dataInput) throws IOException {
		DoubleWritable sv_writable = new DoubleWritable();

		mint = WritableUtils.readVInt(dataInput);
		maxt = WritableUtils.readVInt(dataInput);
		n_of_points = WritableUtils.readVInt(dataInput);
		sv_writable.readFields(dataInput);
		sum_voting = sv_writable.get();


	}

	public void write(DataOutput dataOutput) throws IOException {
		DoubleWritable sv_writable = new DoubleWritable();

		WritableUtils.writeVInt(dataOutput, mint);
		WritableUtils.writeVInt(dataOutput, maxt);
		WritableUtils.writeVInt(dataOutput, n_of_points);
		sv_writable.set(sum_voting);
		sv_writable.write(dataOutput);

		

	}
	
	 @Override
	    public boolean equals(Object o) {

	        if (o == this) return true;
	        if (!(o instanceof DTJSubtrajValueShort)) {
	            return false;
	        }

	        DTJSubtrajValueShort pair = (DTJSubtrajValueShort) o;

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
	    
	public int compareTo(DTJSubtrajValueShort objKeyPair) {


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


}