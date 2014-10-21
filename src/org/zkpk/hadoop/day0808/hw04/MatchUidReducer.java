package org.zkpk.hadoop.day0808.hw04;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatchUidReducer extends Reducer<Text, Text, Text, Text> {
	private  static final Text nullkey=null;
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		int count=0;
		for(Text value:values){
			count++;
		}
		if(count>3){
			context.write(nullkey,key);
		}
	}
}
