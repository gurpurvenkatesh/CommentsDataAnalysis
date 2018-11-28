import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommentsMapper extends Mapper<Object, Text, Text, CommentsTuple>{
	
		/* Input key can be anything. Input value is in the format of text since we do not have default xml input format. 
		   So the input will be read as Text & then XML parser is used to parse the xml parameters */
		
		private CommentsTuple outTuple = new CommentsTuple();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			Map<String, String> parsed = CommentsUtils.transformXmlToMap(value.toString());	/* Input text xml is parsed for each line of comments */
			
			String strDate = parsed.get("CreationDate");		/* Get the CreationDate field from which we are going to find the first & last commented date from map */
			String userId = parsed.get("userId");				/* Get the userid which is used for grouping from map */
			
			if(strDate == null || userId ==null) {				/*If there is no record then return */ 
				return;
			}
			
			DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
			
			try {
				Date creationDate = df.parse(strDate);		/* Parse the String to the date object */
				outTuple.setMin(creationDate);					/* Setting both min & max to th e creationDate */
				outTuple.setMax(creationDate);
				outTuple.setCount(1);							/* 1 count of comment for 1 input split */
				
				Text outUserId = new Text();
				outUserId.set(userId);
				
				context.write(outUserId, outTuple);				/* Map function output with userId as key & the class object with 3 class variables in it as value */
				
			}catch(ParseException e) {
				
			}
		}
		
	
}
