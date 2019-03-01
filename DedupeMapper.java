 package solution;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DedupeMapper extends Mapper<LongWritable, Text, Text, Text> {
@Override
protected void map(LongWritable key,Text value,Mapper<LongWritable,Text,Text,Text>.Context context) throws IOException,InterruptedException{
	
	String line=value.toString();
	String[] parts=line.split("\t");
	
	String host=parts[1];
	context.write(new Text(host),value);
	
	
	 
	
}

}
