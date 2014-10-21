package org.zkpk.hadoop.day0806.hw02;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<Text, DoubleWritable, Text,DoubleWritable> {
	private Text percent =new Text("percent");
	private DoubleWritable result=new DoubleWritable();
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,Context context)
			throws IOException, InterruptedException {
		double sum=0;
		for(DoubleWritable value:values){
			sum+=value.get();
		}
		result.set(sum);
		context.write(percent, result);
	}
}
