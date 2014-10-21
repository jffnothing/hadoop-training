package org.zkpk.hadoop.day0815.hw01;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3Reducer extends Reducer<Text,Text,Text,Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		boolean flag1=false;
		boolean flag2=false;
		for(Text value:values){
			if(value.toString().equals("1")){
				flag1=true;
			}
			if(value.toString().equals("2")){
				flag2=true;
			}
		}
		if(flag1==true&&flag2==true){
			context.write(key,new Text(""));
		}
	}
}
