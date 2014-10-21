package org.zkpk.hadoop.day0808.hw05;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTwoMapper extends Mapper<Object, Text, Text, Text> {
	private Text uid=new Text();
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String [] arr=value.toString().split("\t",-1);
		String keyword=arr[2];
		if(keyword.indexOf("ÏÉ½£ÆæÏÀ´«")!=-1){
			uid.set(arr[1]);
			context.write(uid,value);
		}
	}

}
