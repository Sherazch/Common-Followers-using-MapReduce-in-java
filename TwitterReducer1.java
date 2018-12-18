package HadoopAssignment.Assignment2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class TwitterReducer1 extends
    Reducer<Text, Text, Text, Text> {
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
   StringBuilder followers=new StringBuilder();
	  for(Text v:values) {
		  followers.append(v+",");
	  }
	  String temp=followers.toString();
	  int len=temp.length();
	  temp=temp.substring(0, len-1);
	  String[] splits=temp.split(",");
	  int limit=splits.length;
	  for (int i=0;i<limit-1;i++) {
		  for(int j=i+1;j<limit;j++) {
			  int first,second;
			  if(Integer.parseInt(splits[i])>Integer.parseInt(splits[j])) {
				  first=Integer.parseInt(splits[i]);
				  second=Integer.parseInt(splits[j]);
				  }else {
					  first=Integer.parseInt(splits[j]);
					  second=Integer.parseInt(splits[i]);  
				  }
	  context.write(new Text(second+" & "+first+" ->"), new Text(key));
		  }
	  }
  }
}