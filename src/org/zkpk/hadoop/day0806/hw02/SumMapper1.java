package org.zkpk.hadoop.day0806.hw02;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SumMapper1 extends Mapper<Object, Text, Text, DoubleWritable> {
	private Text perc=new Text();
	private DoubleWritable cou=new DoubleWritable();
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String [] arr=value.toString().split("\t",-1);
		String percent=arr[0];
		double count=(double)Double.parseDouble(arr[1]);
		if(arr.length==2){
			perc.set(percent);
			cou.set(count);
			context.write(perc, cou);
		}
	}

	
}
