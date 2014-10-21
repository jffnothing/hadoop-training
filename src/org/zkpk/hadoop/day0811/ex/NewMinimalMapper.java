package org.zkpk.hadoop.day0811.ex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NewMinimalMapper extends Mapper<Object, Text, Text, IntWritable> {

	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String [] arr=value.toString().split("\t",-1);
		String uid=arr[1];
		if(arr.length==6){
			context.write(new Text(uid),new IntWritable(1));
		}
	}
	
}
