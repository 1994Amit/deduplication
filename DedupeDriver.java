package solution;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DedupeDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception{
		String input, output;
		if(args.length==2) {
			input = args[0];
			output = args[1]; 
		}
		else {
			 	
			System.err.println("Expected: input output");
			return -1;
		}
		
		Job job=Job.getInstance();
		job.setJarByClass(DedupeDriver.class);
		job.setJobName("card Deduper");
		FileInputFormat.setInputPaths(job,new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output));
		
		job.setMapperClass(DedupeMapper.class);
		job.setReducerClass(DedupeReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		

		
		boolean success=job.waitForCompletion(true);
	return success ? 0 : 1 ; 
	}
	
	public static void main(String[] args) throws Exception {
		DedupeDriver dedupeDriver=new DedupeDriver();
		int exitCode=ToolRunner.run(dedupeDriver,args);
		System.exit(exitCode);
	}
}
