package org.zkpk.hadoop.day0820FirstTest.test3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProvinceMarketCount extends Configured implements Tool {

	static class ProvinceMarketCountMapper extends Mapper<Object,Text,Text,Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String [] arr=value.toString().split("\t");
			if(arr.length==6){
				context.write(new Text(arr[4]),new Text(arr[3]));
			}
		}
	}
	static class ProvinceMarketCountReducer extends Reducer<Text,Text,Text,IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Map<String,Integer> map=new HashMap();
			//去重，用set要比map占的空间要小，set无序不可重复
			for(Text value:values){
				if(map.get(value.toString())==null){
					map.put(value.toString(),1);
				}
			}
			context.write(key,new IntWritable(map.size()));
		}	
	}
	static class SortMapper extends Mapper<Object,Text,IntWritable,Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String [] arr=value.toString().split("\t");
			if(arr.length==2){
				context.write(new IntWritable(Integer.parseInt(arr[1])),new Text(arr[0]));
			}
		}	
	}
	static class SortReducer extends Reducer<IntWritable,Text,Text,IntWritable>{
		ArrayList<Integer> list1=new ArrayList();//list1里是从小到大排列的count
		Map<Integer,ArrayList<String>> map=new HashMap();//map里放count和省份
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			list1.add(key.get());
			ArrayList<String> list2=new ArrayList();
			for(Text value:values){
				list2.add(value.toString());
			}
			map.put(new Integer(key.get()),list2);
		}
		/*涉及到排序，要想到1.mr的特性
		              2.重写IntWritable的compareRetor方法，加一个负号成降序
		              3.用到集合
		              4.要反序可用shell实现
		*/
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			for(int i=list1.size()-1;i>=0;i--){
				ArrayList<String> list3=map.get(list1.get(i));
				for(String province:list3){
					context.write(new Text(province),new IntWritable(list1.get(i).intValue()));
				}
			}
		}
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new ProvinceMarketCount(),args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job1=new Job();
		job1.setJarByClass(ProvinceMarketCount.class);
		job1.setMapperClass(ProvinceMarketCountMapper.class);
		job1.setReducerClass(ProvinceMarketCountReducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job1,new Path(args[0]));
		FileOutputFormat.setOutputPath(job1,new Path("hdfs://192.168.32.128:9000/test3_1"));
		job1.waitForCompletion(true);
		
		
		
		
		Job job2=new Job();
		job2.setJarByClass(ProvinceMarketCount.class);
		job2.setMapperClass(SortMapper.class);
		job2.setReducerClass(SortReducer.class);
		
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job2,new Path("hdfs://192.168.32.128:9000/test3_1"));
		FileOutputFormat.setOutputPath(job2,new Path(args[1]));
		return job2.waitForCompletion(true)?0:1;
	}
}
