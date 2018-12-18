package HadoopAssignment.Assignment2;


import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.*;

public class TwitterMapper2 extends
    Mapper<LongWritable, Text, Text, Text> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException{
  
	  String[] splits = value.toString().split(">");
	  String ids=splits[0],followers=splits[1];
	  context.write(new Text(ids), new Text(followers));
  
	  }
    }
  

