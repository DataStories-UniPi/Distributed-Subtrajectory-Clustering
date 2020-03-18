package DataTypes;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class Period implements Writable, WritableComparable<Period> {
	public int ti;
	public int te;
	
	public Period() {
	    this(0, 0);
	  }
	
	public Period(Period b){
		 this(b.ti, b.te);
	}
	
	public Period(int ti, int te) {
		this.setPeriod(ti, te);
	}
	
	@Override
	public String toString() {

		return (new StringBuilder().append(ti).append(",").append(te)).toString();
	}
	
	public void readFields(DataInput dataInput) throws IOException {
		ti = WritableUtils.readVInt(dataInput);
		te = WritableUtils.readVInt(dataInput);

	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeVInt(dataOutput, ti);
		WritableUtils.writeVInt(dataOutput, te);
	}
	
	 @Override
	    public boolean equals(Object o) {

	        if (o == this) return true;
	        if (!(o instanceof Period)) {
	            return false;
	        }

	        Period per = (Period) o;

	        return per.ti == ti && per.te == te;
	    }

	    //Idea from effective Java : Item 9
	    @Override
	    public int hashCode() {
	        int result = 17;
	        result = 31 * result + ti;
	        result = 31 * result + te;

	        return result;
	    }

	public int compareTo(Period o) {
		
		int result = Integer.compare(ti, o.ti);
		
		if (result == 0){
			 
			result =Integer.compare(te, o.te);
		}

		return result;
	
	}
	
	public void setPeriod(int ti, int te) {
		this.ti = ti;
		this.te = te;
	}
	
	public int getPeriod_ti() {
		return ti;
	}

	public int getPeriod_te() {
		return te;
	}
	public void setPeriod_ti(int ti) {
		this.ti = ti;
	}
	
	public void setPeriod_te(int te) {
		this.te = te;
	}

	public int Duration(){
		
		return this.te - this.ti;
		
	}
	
	
	public boolean IntersectsPeriod(Period per){
		
		if (this.ti <= per.te && this.te >= per.ti){
			
			return true;

		} else {
			
			return false;
			
		}
		
	}
	
	public boolean ContainsPeriod(Period per){
		
		if (this.ti <= per.ti && this.te <= per.te){
			
			return true;

		} else {
			
			return false;
			
		}
		
	}
}
