package org.zkpk.hadoop.day0808.hw01;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumURLReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private static final Text nullkey=null;
	private IntWritable result=new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable value:values){
			sum++;
		}
		result.set(sum);
		context.write(nullkey,result);
	}

}
