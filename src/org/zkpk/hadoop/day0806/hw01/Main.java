package org.zkpk.hadoop.day0806.hw01;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=2){
			throw new Exception("please input two arguments");
		}
		Job job1=new Job();
		
		job1.setJarByClass(Main.class);
		job1.setMapperClass(CountMapper.class);
		job1.setReducerClass(CountReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("hdfs://192.168.32.128:9000/term"));
		
		job1.waitForCompletion(true);//ִ�в��˳�
		//System.exit(job1.waitForCompletion(true)?0:1);  �����
		
		Job job2=new Job();
		job2.setJarByClass(Main.class);
		job2.setMapperClass(CountMapper1.class);
		job2.setReducerClass(CountReducer1.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job2, new Path("hdfs://192.168.32.128:9000/term"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true)?0:1);//ִ��
	}

}
