import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class CommentsTuple implements Writable {

	private Date min = new Date();
	private Date max = new Date();
	private long count = 0;
	
	//private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	
	public Date getMin() {
		return min;
	}
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
	public void setMin(Date min) {
		this.min = min;
	}
	public Date getMax() {
		return max;
	}
	public void setMax(Date max) {
		this.max = max;
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}	
}