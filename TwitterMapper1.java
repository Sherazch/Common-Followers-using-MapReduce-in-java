package HadoopAssignment.Assignment2;


import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.*;

public class TwitterMapper1 extends
    Mapper<LongWritable, Text, Text, Text> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException{
  
	  String[] splits = value.toString().split(" ");
	  String uid1=splits[0],uid2=splits[1];
	  context.write(new Text(uid1), new Text(uid2));
  
	  }
    }
  

