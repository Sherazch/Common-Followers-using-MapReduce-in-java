package HadoopAssignment.Assignment2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class TwitterReducer2 extends
    Reducer<Text, Text, Text, Text> {
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
   
	  StringBuilder tmp=new StringBuilder();
	  for(Text v:values) {
		  tmp.append(v+",");
	  }   
	  String commonFollowers=tmp.toString();
	  commonFollowers=commonFollowers.substring(0, commonFollowers.length()-1);
	  context.write(key, new Text("{"+commonFollowers+"}"));
	  
  }
}