package org.tweaktalent.hadoop.airline;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class AirLineMap extends Mapper<LongWritable,Text,IntWritable,Text>{

//	private final static IntWritable one = new IntWritable(1);
//    private Text word = new Text();
//	
//	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
//	//public void map(IntWritable key,Text value,Context context) throws IOException, InterruptedException {
//		
//		String[] words = value.toString().split("\t");
//		//if(words[0].equalsIgnoreCase("2008")) {
//			//if(words[17].equalsIgnoreCase("BHM")|| words[17].equalsIgnoreCase("BNA")||words[17].equalsIgnoreCase("cec"))
//			//{
//			context.write(new Text(words[2]),new IntWritable(1));
//		//}
//	//}
//		
//		//String line = value.toString();
//       // StringTokenizer tokenizer = new StringTokenizer(line);
//        //while (tokenizer.hasMoreTokens()) {
//         //   word.set(tokenizer.nextToken());
//          //  context.write(word, one);
//       // }
	//}
	final static Logger logger = Logger.getLogger(AirLineCount.class);
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		
		// Name,age,gender,marks
		String[] words = value.toString().split(",");
		int age = Integer.parseInt(words[2]);
		logger.info("Key:"+age+" Value:"+value);
		context.write(new IntWritable(age), value);
}
	
}
