package solution;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DedupeReducer extends Reducer<Text, Text, Text,Text> {
	@Override
	public void reduce(Text key,Iterable<Text> values,Reducer<Text,Text,Text,Text>.Context context) 	throws IOException,InterruptedException{
		int highestheartnum=-1;
		String highesthHeartLine=null;
		
		
		for(Text text:values) {
	
	String line=text.toString();
	String[] pieces=line.split("\t");
	
	if(pieces[0].equals("burger.letters.com")) {
		try {
				int cardNum=Integer.parseInt(pieces[5]);
				if(cardNum>highestheartnum) {
					highestheartnum=cardNum;
					highesthHeartLine=line;
					
				}
 }
		catch(Exception e) {
			
		}
		}
	}

		
if(highesthHeartLine!=null)		
context.write(key,new Text(highesthHeartLine));
}
	
}
