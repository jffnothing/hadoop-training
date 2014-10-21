package org.zkpk.hadoop.day0806.hw03;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ShowReducer extends Reducer<Text, Text, Text, Text> {
	private static final Text NullResult=null;
	private Text result=new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		int sum=0;
		ArrayList<Text> cache=new ArrayList<Text>();
		for(Text value:values){
			sum++;
			Text text=new Text();
			text.set(value.toString());
			cache.add(text);
		}
		if(sum>2){
			int size=cache.size();
			for(int i=0;i<size;i++){
				context.write(NullResult,cache.get(i));
			}
		}
	}	
}
