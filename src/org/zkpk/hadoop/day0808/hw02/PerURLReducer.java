package org.zkpk.hadoop.day0808.hw02;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PerURLReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	double sum=0;
	double count=0;
	private static final Text nullkey=null;
	private DoubleWritable result=new DoubleWritable();
	
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		for(DoubleWritable value:values){
			if(key.toString().matches("^(http://|https://){0,1}([A-Za-z]+\\.){0,1}[A-Za-z0-9]+(\\.[A-Za-z]+){1,}$")){
				count++;
			}
			sum++;
		}
	}

	@Override
	public void run(Context context)
			throws IOException, InterruptedException {
		super.run(context);
		//result.set("总的搜索记录的个数："+sum+"\n"+"直接输入URL做为关键字的个数："+count+"\n"+"百分比:"+((count/sum)*100)+"%");
		result.set(count/sum);
		context.write(nullkey, result);	
	}


}
