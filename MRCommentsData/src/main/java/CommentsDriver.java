import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CommentsDriver extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf);	/* used for job submitting, checking job state etc */
		
		job.setJarByClass(CommentsDriver.class);		/* This line let the data node to find for mapper & reducer in to this class */
		
		/* Input & Output path is sent through the command line argument */
		FileInputFormat.addInputPath(job, new Path(args[0]));	/* default input format is TextInputFormat which extends FileInputFormat. So we used here FileInputFormat */
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	
		
		job.setMapperClass(CommentsMapper.class);		/* Setting Mapper class */
				
		job.setReducerClass(CommentsReducer.class);	/* Setting Reducer class */
		
		job.setMapOutputKeyClass(Text.class);		/* Setting Mapper Output key class */
		job.setMapOutputValueClass(CommentsTuple.class);	/* Setting Mapper Output Value class */
		job.setOutputKeyClass(Text.class);			/* Setting Final Output key class */
		job.setOutputValueClass(CommentsTuple.class);		/* Setting Final Output Value class */
		job.setNumReduceTasks(1);
		
		return (job.waitForCompletion(true) ? 0 : 1);
		
	}
	
	public static void main(String args[]) throws Exception {
	    int res = ToolRunner.run(new CommentsDriver(), args); /* Execute the command with the given args command line arguments */
	    System.exit(res);
	}

}
