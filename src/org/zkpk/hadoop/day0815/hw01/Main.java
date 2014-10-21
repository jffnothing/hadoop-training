package org.zkpk.hadoop.day0815.hw01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=2){
			throw new Exception("please input two argments!");
		}
		Configuration job1conf=new Configuration();
		Job job1=new Job(job1conf,"job1");
		job1.setJarByClass(Main.class);
		job1.setMapperClass(Job1Mapper.class);
		job1.setNumReduceTasks(0);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Text.class);
		
		ControlledJob ajob1=new ControlledJob(job1conf);
		ajob1.setJob(job1);
		
		FileInputFormat.setInputPaths(job1,new Path(args[0]));
		FileOutputFormat.setOutputPath(job1,new Path("hdfs://192.168.32.128:9000/cache/job1"));
		
		//job1.waitForCompletion(true);
		
		Configuration job2conf=new Configuration();
		Job job2=new Job(job2conf,"job2");
		job2.setJarByClass(Main.class);
		job2.setMapperClass(Job2Mapper.class);
		job2.setNumReduceTasks(0);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Text.class);
		
		ControlledJob ajob2=new ControlledJob(job2conf);
		ajob2.setJob(job2);
		
		FileInputFormat.setInputPaths(job2,new Path(args[0]));
		FileOutputFormat.setOutputPath(job2,new Path("hdfs://192.168.32.128:9000/cache/job2"));
		//job2.waitForCompletion(true);
		
		Configuration job3conf=new Configuration();
		Job job3=new Job(job3conf,"job3");
		job3.setJarByClass(Main.class);
		
		ControlledJob ajob3=new ControlledJob(job3conf);
		ajob3.setJob(job3);
		
		MultipleInputs.addInputPath(job3, new Path("hdfs://192.168.32.128:9000/cache/job1"),TextInputFormat.class,Job3UrlMapper.class);
		MultipleInputs.addInputPath(job3, new Path("hdfs://192.168.32.128:9000/cache/job2"),TextInputFormat.class,Job3KeywordMapper.class);
		job3.setReducerClass(Job3Reducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job3,new Path(args[1]));
		//System.exit(job3.waitForCompletion(true)?0:1);
		
		ajob3.addDependingJob(ajob1);
		ajob3.addDependingJob(ajob2);
		
		JobControl JC=new JobControl("jc");
		JC.addJob(ajob1);
		JC.addJob(ajob2);
		JC.addJob(ajob3);
		
		Thread t=new Thread(JC);
		t.start();
		
		while(true){
			if(JC.allFinished()){
				System.out.println(JC.getSuccessfulJobList());
				JC.stop();
				break;
			}
			if(JC.getFailedJobList().size()>0){
				System.out.println(JC.getFailedJobList());
				JC.stop();
				break;
			}
		}
		//JC.run();
	}

}
