package org.zkpk.hadoop.day0812.ex;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WholeFileMain {
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=2){
			throw new Exception("please input two argments!");
		}
		Job job=new Job();
		job.setJarByClass(WholeFileMain.class);
		job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(IdentityReducer.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setInputFormatClass(WholeFileInputFormat.class);
		
		WholeFileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
