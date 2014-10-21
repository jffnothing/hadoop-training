package org.zkpk.hadoop.day0820FirstTest.cleanData;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MarketProvinceCity extends Configured implements Tool  {
		static class MarketProvinceCityMapper extends Mapper<Object,Text,Text,Text>{

			@Override
			protected void map(Object key, Text value, Context context)
					throws IOException, InterruptedException {
				String [] arr=value.toString().split("\t");
				if(arr.length==6){
					context.write(new Text(arr[3]),new Text(arr[4]+"\t"+arr[5]));
				}
			}
		}
		static class MarketProvinceCityReducer extends Reducer<Text,Text,Text,Text>{

			@Override
			protected void reduce(Text key, Iterable<Text> values,Context context)
					throws IOException, InterruptedException {
				context.write(key,values.iterator().next());
			}
		}
		@Override
		public int run(String[] args) throws Exception {
			// TODO Auto-generated method stub
			Job job=NewJobBuilder.parseInputAndOutput(this, getConf(), args);
			job.setJarByClass(MarketProvinceCity.class);
			job.setMapperClass(MarketProvinceCityMapper.class);
			job.setReducerClass(MarketProvinceCityReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			return job.waitForCompletion(true)?0:1;
		}
		public static void main(String[] args) throws Exception {
			// TODO Auto-generated method stub
			int exitCode=ToolRunner.run(new MarketProvinceCity(),args);
			System.exit(exitCode);
		}
	}
