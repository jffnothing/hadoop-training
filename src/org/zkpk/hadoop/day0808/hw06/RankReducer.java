package org.zkpk.hadoop.day0808.hw06;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RankReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
	Map<Integer,Double> map=new HashMap();
	private IntWritable rank=new IntWritable();
	private DoubleWritable result=new DoubleWritable();
	@Override
	protected void reduce(IntWritable key, Iterable<DoubleWritable> values,Context context)
			throws IOException, InterruptedException {
		double count=0;
		for(DoubleWritable value:values){
			count++;
		}
		map.put(key.get(), count);
	}

	@Override
	public void run(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		double sum=0;
		double percent=0;
		super.run(context);
		for(Map.Entry<Integer,Double> entry:map.entrySet()){
			sum+=entry.getValue();
		}
		for(Map.Entry<Integer,Double> entry:map.entrySet()){
			percent=entry.getValue()/sum;
			rank.set(entry.getKey());
			result.set(percent);
			context.write(rank,result);
		}
		
	}
	
}
