package org.zkpk.hadoop.day0806.hw03;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ShowMapper extends Mapper<Object, Text, Text, Text> {
	private Text uid=new Text();
	private Text one=new Text();
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String [] arr =value.toString().split("\t",-1);
		if (arr.length==6){
			uid.set(arr[1]);
			one.set(value.toString());
			context.write(uid,one);
		}
	}
}