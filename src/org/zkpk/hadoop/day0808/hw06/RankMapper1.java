package org.zkpk.hadoop.day0808.hw06;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankMapper1 extends Mapper<Object, Text, IntWritable, Text> {
	private IntWritable rank=new IntWritable();
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String [] arr=value.toString().split("\t",-1);
		rank.set(Integer.parseInt(arr[3]));
		context.write(rank, value);
	}
	
}
