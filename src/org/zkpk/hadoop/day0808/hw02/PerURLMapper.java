package org.zkpk.hadoop.day0808.hw02;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PerURLMapper extends Mapper<Object,Text,Text, DoubleWritable> {
	private Text mark=new Text();
	private DoubleWritable one=new DoubleWritable(1);
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String [] arr=value.toString().split("\t",-1);
		if(arr.length==6){
			mark.set(arr[2]);
			context.write(mark, one);
		}	
	}

}
