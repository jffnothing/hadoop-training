package org.zkpk.hadoop.day0815.hw01;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper extends Mapper<Object,Text,NullWritable,Text> {

	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String [] arr=value.toString().split("\t",-1);
		if(arr.length==6){
			String keyword=arr[2];
			if(keyword.indexOf("°Ù¶È")!=-1){
				context.write(NullWritable.get(),value);
			}
		}
	}
}
