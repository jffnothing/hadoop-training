package org.zkpk.hadoop.day0808.hw06;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RankReducer1 extends Reducer<IntWritable,Text,IntWritable,Text> {
	private IntWritable rank=new IntWritable();
	private Text result=new Text();
	Map<Integer,Double> map=new HashMap();
	double sum=0;
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		double count=0;
		for(Text value:values){
			String [] arr=value.toString().split("\t",-1);
			if(Integer.parseInt(arr[3])<=10){
				count++;
			}
			sum++;
		}
		if(count!=0){
			map.put(key.get(), count);
		}
	}

	@Override
	public void run(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.run(context);
		double percent=0;
		for(Map.Entry<Integer,Double> entry:map.entrySet()){
			percent=(entry.getValue()/sum)*100;
			rank.set(entry.getKey());
			result.set(percent+"%");
			context.write(rank,result);
		}
	}
	
}
