package HadoopAssignment.Assignment2;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;


public class CommonFollowersDriver {
	
	public static void main(String[]args) throws Exception{
		
		Configuration config=new Configuration();
		
		Job job1=Job.getInstance(config, "Common Followers Task");
		job1.setJarByClass(CommonFollowersDriver.class);
		job1.setMapperClass(TwitterMapper1.class);
		job1.setReducerClass(TwitterReducer1.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		Path tempOut=new Path("temp");
		SequenceFileOutputFormat.setOutputPath(job1, tempOut);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.waitForCompletion(true);
		
		
		Job job2=new Job(config,"Group By Followers");
		job2.setJarByClass(CommonFollowersDriver.class);
		//job2.setMapperClass(TwitterMapper2.class);
		job2.setReducerClass(TwitterReducer2.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job2, tempOut);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.waitForCompletion(true);
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
		
	}
	

}

