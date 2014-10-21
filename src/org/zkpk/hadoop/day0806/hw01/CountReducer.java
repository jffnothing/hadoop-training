package org.zkpk.hadoop.day0806.hw01;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result=new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context arg2)
			throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable value:values){
			sum+=value.get();
		}
		result.set(sum);
		arg2.write(key,result);
	}	
}
/*uid   count
 * 1      2
 * 2      4
 * 3      3
 */
