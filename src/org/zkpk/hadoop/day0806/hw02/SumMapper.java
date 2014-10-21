package org.zkpk.hadoop.day0806.hw02;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SumMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	private Text sumRecord=new Text("sumRecord");
	private Text gt2people=new Text("gt2people");
	private DoubleWritable one=new DoubleWritable(1);
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String [] arr=value.toString().split("\t",-1);
		double count=(double)Integer.parseInt(arr[1]);
		if(arr.length==2){
			if(count>2){
				context.write(gt2people, one);
			}
			context.write(sumRecord, one);
		}
	}
}
