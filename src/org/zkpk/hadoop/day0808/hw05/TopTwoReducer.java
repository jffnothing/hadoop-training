package org.zkpk.hadoop.day0808.hw05;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTwoReducer extends Reducer<Text, Text, Text, Text> {
	private  Text one=new Text("one");
	private  Text two=new Text("two");
	ArrayList<String> list1=new ArrayList();//list1大  不能用Text
	ArrayList<String> list2=new ArrayList();//list2小
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		ArrayList<String> cache=new ArrayList<String>();
		int count=0;
		for(Text value:values){
			count++;
			cache.add(value.toString());
		}
		if(count>3){
			if(cache.size()>list2.size()){
				if(cache.size()>list1.size()){
					list2.clear();
					list2.addAll(list1);
					list1.clear();
					list1.addAll(cache);
				}
				else{
					list2.clear();
					list2.addAll(cache);
				}
			}
		}
	}
	@Override
	public void run(Context context)
			throws IOException, InterruptedException {
		super.run(context);
		for(String value:list1){
			context.write(one, new Text(value));
		}
		for(String value:list2){
			context.write(two, new Text(value));
		}
	}
	
}
