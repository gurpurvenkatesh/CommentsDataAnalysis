import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommentsReducer extends Reducer<Text, CommentsTuple, Text, CommentsTuple>{
	
	private CommentsTuple result = new CommentsTuple();
	
	public void reduce(Text key, Iterable<CommentsTuple> values, Context context) throws IOException, InterruptedException {
		
		result.setMin(null);
		result.setMax(null);
		int sum =0;
		
		for(CommentsTuple val : values) {		/* Iterating through the map */
			
			if(result.getMin() == null || val.getMin().compareTo(result.getMin()) < 0) {		/* Comparing the minimum date & setting the minimum date to the final result */
				result.setMin(val.getMin());
			}
			
			if(result.getMax() == null || val.getMax().compareTo(result.getMax()) > 0) {		/* Comparing the maximum date & setting the mimum date to the final result */
				result.setMax(val.getMax());
			}
			
			sum += val.getCount();		/* Total sum of comments */
		}
		
		result.setCount(sum);
		context.write(key, result);
		
	}


}
