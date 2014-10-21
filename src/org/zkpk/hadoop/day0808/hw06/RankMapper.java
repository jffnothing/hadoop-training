package org.zkpk.hadoop.day0808.hw06;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
	private IntWritable rank=new IntWritable();
	private DoubleWritable one=new DoubleWritable(1);
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String [] arr=value.toString().split("\t",-1);
		int ran=Integer.parseInt(arr[3]);
		if(ran<=10){
			rank.set(ran);
			context.write(rank, one);
		}
	}

}
