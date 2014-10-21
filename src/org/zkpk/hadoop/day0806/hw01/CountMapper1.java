package org.zkpk.hadoop.day0806.hw01;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper1 extends Mapper<Object, Text, Text, IntWritable> {
	private Text people=new Text("people");
	private IntWritable one=new IntWritable(1);
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String [] arr=value.toString().split("\t",-1);
		int count=Integer.parseInt(arr[1]);
		if(arr.length==2 && count>2 ){//ѡ����¼����2��
			context.write(people, one);
		}
	}
}
/*
  people       one
   people       1
   people       1
 */
