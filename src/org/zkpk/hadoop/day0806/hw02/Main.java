package org.zkpk.hadoop.day0806.hw02;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=1){
			throw new Exception("please input two arguments");
		}
		Job job1=new Job();
		job1.setJarByClass(Main.class);
		job1.setMapperClass(SumMapper.class);
		job1.setReducerClass(SumReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.setInputPaths(job1, new Path("hdfs://192.168.32.128:9000/term"));
		FileOutputFormat.setOutputPath(job1, new Path("hdfs://192.168.32.128:9000/term1"));
		
		job1.waitForCompletion(true);
		
		Job job2=new Job();
		job2.setJarByClass(Main.class);
		job2.setMapperClass(SumMapper1.class);
		job2.setReducerClass(SumReducer1.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.setInputPaths(job2, new Path("hdfs://192.168.32.128:9000/term1"));
		FileOutputFormat.setOutputPath(job2, new Path(args[0]));
		System.exit(job2.waitForCompletion(true)?0:1);
	}

}
