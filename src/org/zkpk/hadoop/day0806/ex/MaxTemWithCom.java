package org.zkpk.hadoop.day0806.ex;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class MaxTemWithCom {
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=2){
			throw new Exception("please put two arguments");
		}
		JobConf job=new JobConf(MaxTemWithCom.class);
		job.setJobName("Max Tem");
		job.setJarByClass(MaxTemWithCom.class);
		job.setMapperClass(MaxMapper.class);
		job.setCombinerClass(MaxReducer.class);
		job.setReducerClass(MaxReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setBoolean("mapred.output.compress", true);
		job.setClass("mapred.output.compression.codec", GzipCodec.class,CompressionCodec.class);
		
		JobClient.runJob(job);
	}

}
