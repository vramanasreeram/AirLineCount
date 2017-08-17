package org.tweaktalent.hadoop.airline;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirLineMap extends Mapper<LongWritable,Text,Text,IntWritable>{

	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
	//public void map(IntWritable key,Text value,Context context) throws IOException, InterruptedException {
		
		String[] words = value.toString().split("\t");
		//if(words[0].equalsIgnoreCase("2008")) {
			//if(words[17].equalsIgnoreCase("BHM")|| words[17].equalsIgnoreCase("BNA")||words[17].equalsIgnoreCase("cec"))
			//{
			context.write(new Text(words[2]),new IntWritable(1));
		//}
	//}
		
		//String line = value.toString();
       // StringTokenizer tokenizer = new StringTokenizer(line);
        //while (tokenizer.hasMoreTokens()) {
         //   word.set(tokenizer.nextToken());
          //  context.write(word, one);
       // }
	}
}
