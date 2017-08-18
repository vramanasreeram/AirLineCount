package org.tweaktalent.hadoop.airline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;


//public class AirLineReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
//
// Logger logger = Logger.getLogger(AirLineReduce.class);
// public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
//  
//  int sum =0;
//  for(IntWritable value:values) {
//  System.out.println("Key:"+ key.toString() + " Values:"+value.get());
//   //logger.info("Key:"+ key.toString() + " Values:"+value.get());
//   sum+= value.get();
//  }
//  context.write(key, new IntWritable(sum));
// }
//}

public class AirLineReduce extends Reducer<IntWritable,Text,NullWritable,Text>{
	final static Logger logger = Logger.getLogger(AirLineCount.class);
	@Override
	public void reduce(IntWritable key,Iterable<Text>values,Context context) throws IOException, InterruptedException{
		int maxMarks = Integer.MIN_VALUE;
		Text val = new Text();
		for(Text value:values){
			int marks = Integer.parseInt(value.toString().split(",")[1]);
			logger.info("Marks:"+marks);
			if(marks > maxMarks){
				maxMarks = marks;
				val = value;
			}
			logger.info("MxMarks:"+maxMarks+" Value:"+val.toString());
		}
		logger.info("Reducer:"+val);
		context.write(NullWritable.get(), val);
		
	}
	
}


 

