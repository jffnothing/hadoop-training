package org.zkpk.hadoop.day0820FirstTest.test4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ProvinceProductCount extends Configured implements Tool {

	static class ProvinceProductCountMapper extends Mapper<Object,Text,Text,Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] arr=value.toString().split("\t",-1);
			if(arr.length==6){
				boolean b=true;
				for(String str:arr){
					if(str.equals(null)||str.equals("")){
						b=false;
						break;
					}
				}
				if(b){
					context.write(new Text(arr[4]), new Text(arr[0]));
				}
			}
		}
		
	}
	
	static class ProvinceProductCountReducer extends Reducer<Text,Text,Text,IntWritable>{

		HashSet<String> top1=new HashSet();
		HashSet<String> top2=new HashSet();
		HashSet<String> top3=new HashSet();
		HashMap<Integer,ArrayList<String>> maps=new HashMap();
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			HashSet<String> set = new HashSet();
			for(Text value:values){
				set.add(value.toString());
			}
			if(maps.get(set.size())==null){
				ArrayList<String> l=new ArrayList();
				l.add(key.toString());
				maps.put(set.size(),l);
			}else{
				ArrayList<String> l=new ArrayList();
				l.addAll(maps.get(set.size()));
				l.add(key.toString());
				maps.put(set.size(), l);
			}
			
			
			if(set.size()>top3.size()){
				if(set.size()>top2.size()){
					if(set.size()>top1.size()){
						top3.clear();
						top3.addAll(top2);
						top2.clear();
						top2.addAll(top1);
						top1.clear();
						top1.addAll(set);
					}else{
						top3.clear();
						top3.addAll(top2);
						top2.clear();
						top2.addAll(set);
					}
				}else{
					top3.clear();
					top3.addAll(set);
				}
			}
//			context.write(key, new IntWritable(set.size()));
		}
		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Integer[] sort=maps.keySet().toArray(new Integer[maps.size()]);
			Arrays.sort(sort);
			for(int i=sort.length-1;i>=0;i--){
				for(String str:maps.get(sort[i])){
					context.write(str, sort[i]);
				}
			}
			
			HashSet<String> top=null;
			for(String s1:top1){
				for(String s2:top2){
					for(String s3:top3){
						if(s1.equals(s2)&&s2.equals(s3)){
							context.write(s3,null);
						}
					}
				}
			}
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=JobBuilderNew.parseInputAndOutput("", args);
		if(job==null){
			return -1;
		}
		job.setJarByClass(ProvinceProductCount.class);  
		job.setMapperClass(ProvinceProductCountMapper.class);
		job.setReducerClass(ProvinceProductCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new ProvinceProductCount(), args);
		System.exit(exitCode);
	}

}
