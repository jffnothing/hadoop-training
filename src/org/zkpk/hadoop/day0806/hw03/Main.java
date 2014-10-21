package org.zkpk.hadoop.day0806.hw03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.zkpk.hadoop.day0806.hw02.SumMapper;
import org.zkpk.hadoop.day0806.hw02.SumReducer;

public class Main {
	public static void main(String[] args) throws Exception {
		if(args.length!=2){
			throw new Exception("please put two argments!");
		}
		Job job=new Job();
		job.setJarByClass(Main.class);
		job.setMapperClass(ShowMapper.class);
		job.setReducerClass(ShowReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.32.128:9000/term2"));
		
		job.waitForCompletion(true);
		
		Job job1=new Job();
		job1.setJarByClass(Main.class);
		job1.setMapperClass(ShowMapper1.class);
		job1.setReducerClass(ShowReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job1,"hdfs://192.168.32.128:9000/term2");
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		System.exit(job1.waitForCompletion(true)?0:1);
	}
}
