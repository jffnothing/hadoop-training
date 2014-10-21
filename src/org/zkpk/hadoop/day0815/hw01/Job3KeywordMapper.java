package org.zkpk.hadoop.day0815.hw01;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job3KeywordMapper extends Mapper<Object,Text,Text,Text> {

	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String [] arr=value.toString().split("\t",-1);
		String uid=arr[1];
		context.write(new Text(uid),new Text("2"));
	}

}
