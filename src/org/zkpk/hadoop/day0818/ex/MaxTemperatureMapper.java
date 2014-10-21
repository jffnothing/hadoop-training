package org.zkpk.hadoop.day0818.ex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;

public class MaxTemperatureMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	private static final int MISSING=9999;
	@Override
	public void map(LongWritable key, Text value,Context context)
			throws IOException {
		String line=value.toString();
		String year=line.substring(15,19);
		int airTem;
		if(line.charAt(87)=='+'){
			airTem=Integer.parseInt(line.substring(88,92));
		}else{
			airTem=Integer.parseInt(line.substring(87,92));
		}
		String quality=line.substring(92,93);
		if(airTem!=MISSING && quality.matches("[01459]")){
			try {
				context.write(new Text(year),new IntWritable(airTem));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
}
