package org.zkpk.hadoop.day0808.hw03;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PerSameURLMapper extends Mapper<Object,Text,Text, Text> {
	double sum=0;
	double count=0;
	private Text mark=new Text("mark");
	private Text result =new Text();
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String [] arr=value.toString().split("\t",-1);
		String keyword=arr[2];
		if(arr.length==6){
			if(keyword.matches("^(http://|https://){0,1}([A-Za-z]+\\.){0,1}[A-Za-z0-9]+(\\.[A-Za-z]+){1,}$")){
				context.write(mark,value);
			}
		}
	}	
}
