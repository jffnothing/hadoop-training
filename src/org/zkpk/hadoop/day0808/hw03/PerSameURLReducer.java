package org.zkpk.hadoop.day0808.hw03;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PerSameURLReducer extends Reducer<Text, Text, Text, Text> {
	double sum=0;
	double count=0;
	private static final Text nullkey=null;
	private Text result=new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		for(Text value:values){
			String [] arr=value.toString().split("\t",-1);
			String keyword=arr[2];
			String url=arr[5];
			if(arr.length==6){
				sum++;
				if(url.indexOf(keyword)!=-1){
					count++;
				}
			}
		}
	}
	@Override
	public void run(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.run(context);
		double per=(count/sum)*100;
		result.set("直接输入URL作为关键词的个数："+sum+"\t"+"关键字与URL匹配的个数："+count+"\t"+"所占百分比为："+per+"%");
		context.write(nullkey, result);
	}
}
