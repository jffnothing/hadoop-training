package org.zkpk.hadoop.day0808.hw06;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main1 {
	public static void main(String[] args) throws Exception {
		if(args.length!=2){
			throw new Exception("please input two argments!");
		}
		Job job=new Job();
		job.setJarByClass(Main.class);
		job.setMapperClass(RankMapper1.class);
		job.setReducerClass(RankReducer1.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
