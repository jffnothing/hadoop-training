package org.zkpk.hadoop.day0818.ex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MaxTemWithCom {
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=2){
			throw new Exception("please put two arguments");
		}
		Configuration conf=new Configuration();
		Job job=new Job(conf);
		job.setJobName("Max Tem");
		job.setJarByClass(MaxTemWithCom.class);
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		新api的hprof的用法		
		conf.setBoolean("mapred.task.profile", true);
		conf.set("mapred.task.profile.params",
               "-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y," +
                 "verbose=n,file=%s");
		boolean isMap = true;
		conf.set((isMap ? "mapred.task.profile.maps" : 
                       "mapred.task.profile.reduces"), "0-2");
	
//		旧api的hprof的用法
//		job.setProfileEnabled(true);
//		job.setProfileParams("-agentlib:hprof=cpu=samples,heap=sites,depth=6,"+"force=n,thread=y,verbose=n,file=%s");
//		job.setProfileTaskRange(true, "0-2");
		
		System.exit(job.waitForCompletion(true)?0:1);
	
		
	}

}
