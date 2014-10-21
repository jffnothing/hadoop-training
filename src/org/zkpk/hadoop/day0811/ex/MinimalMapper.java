package org.zkpk.hadoop.day0811.ex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MinimalMapper extends MapReduceBase implements
		Mapper<LongWritable,Text,Text,IntWritable> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		String [] arr=value.toString().split("\t",-1);
		String uid=arr[1];
		if(arr.length==6){
			output.collect(new Text(uid),new IntWritable(1));
		}
	}

}
