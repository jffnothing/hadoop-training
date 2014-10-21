package org.zkpk.hadoop.day0806.hw03;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ShowReducer1 extends Reducer<Text, Text, Text, Text> {
	private Text mark=null;
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		Map<String,Integer> map=new HashMap(10);
		int n=10;
		for(Text value:values){
			if(n>0){
				String [] arr=value.toString().split("\t",-1);
				String uid=arr[1];
				if(map.get(uid)!=null){
					context.write(mark, value);
				}else{
					map.put(uid,1);
					context.write(mark, value);
					n--;
				}
			}
		}
	}
	
}