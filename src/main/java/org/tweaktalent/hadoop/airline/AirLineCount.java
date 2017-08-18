package org.tweaktalent.hadoop.airline;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class AirLineCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    
//	Configuration conf = new Configuration();
//    Job job = new Job(conf, "AirLine Count");
//    job.setJarByClass(AirLineCount.class);
//    job.setOutputKeyClass(Text.class);
//    job.setOutputValueClass(IntWritable.class);
//    job.setMapperClass(AirLineMap.class);
//    //job.setReducerClass(AirLineReduce.class);
//    job.setPartitionerClass(AirLinePartitioner.class);
//    job.setInputFormatClass(TextInputFormat.class);
//    job.setOutputFormatClass(TextOutputFormat.class);
//    job.setNumReduceTasks(3);
//    FileInputFormat.addInputPath(job, new Path(args[0]));
//    FileOutputFormat.setOutputPath(job, new Path(args[1]));
//    job.waitForCompletion(true);
//    job.submit();
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CustomPartitioner Example");
		job.setJarByClass(AirLineCount.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(AirLineMap.class);
		job.setReducerClass(AirLineReduce.class);
		job.setPartitionerClass(AirLinePartitioner.class);
		job.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.waitForCompletion(true);
	}

}
