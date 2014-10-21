package org.zkpk.hadoop.day0806.hw03;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ShowMapper1 extends Mapper<Object, Text, Text, Text> {
	private Text mark=new Text("mark");
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String [] arr=value.toString().split("\t",-1);
		context.write(mark, value);
	}
	
}
