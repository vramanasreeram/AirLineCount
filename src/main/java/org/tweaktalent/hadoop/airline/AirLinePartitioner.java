package org.tweaktalent.hadoop.airline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

public class AirLinePartitioner extends Partitioner<Text,IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numOfReduceTasks) {
		// TODO Auto-generated method stub
		String[] words = value.toString().split(",");
		int age=Integer.parseInt(words[2]);
		       if(age<=20)
				{
			        return 0;
			
				}
		       else if(20< age && age <= 30)
		       {
		    	   
		    	   return 1 % numOfReduceTasks;
		       }
		       else
		       {
		    	   return 1 % numOfReduceTasks;
		    	   
		       }
		
	}


}
