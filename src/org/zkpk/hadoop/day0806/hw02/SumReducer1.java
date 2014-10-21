package org.zkpk.hadoop.day0806.hw02;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	private static final Text NullWritable = null;
	private DoubleWritable result = new DoubleWritable();
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,Context context)
			throws IOException, InterruptedException {
		double [] n=new double [2];
		int i=0;
		for(DoubleWritable value:values){
			n[i]=value.get();
			i++;
		}
		double end=n[0]/n[1];
		result.set(end);
		if(end<=1){
			context.write(NullWritable,result);
		}else{
			end=n[1]/n[0];
			result.set(end);
			context.write(NullWritable,result);
		}
	}
	
}
