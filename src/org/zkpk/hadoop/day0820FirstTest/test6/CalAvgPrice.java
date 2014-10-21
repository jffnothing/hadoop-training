package org.zkpk.hadoop.day0820FirstTest.test6;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CalAvgPrice {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	static class CalAvgPriceMapper extends Mapper<Object,Text,Text,DoubleWritable>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			ArrayList<Double> list=new ArrayList();
			String [] arr=value.toString().split("\t");
			if(arr.length==6){
				for(int i=1;i<arr.length;i++){
					double cache=Double.parseDouble(arr[i]);
					list.add(cache);
				}
				//System.out.print("--------------"+(list.get(4)));
				int N=context.getConfiguration().getInt("period",3);
				int start=context.getConfiguration().getInt("start.line", 1);
				//System.out.print("--------------"+N+"------------"+start);
				double sum=0;
				for(int i=start;i<start+N;i++){
					sum+=list.get(i-1);
					//System.out.print(sum);
				}
				double calprice=Math.ceil((sum/N)*10+0.5)/10;
				if(start==1){
					context.write(new Text("2014/1/4"),new DoubleWritable(calprice));
				}
				if(start==2){
					context.write(new Text("2014/1/5"),new DoubleWritable(calprice));
				}	
			}
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		int it=2;
		int N=3;
		int startLineNum=1;
		Configuration conf=new Configuration();
		for(int i=0;i<it;i++){
			conf.setInt("period", N);
			conf.setInt("start.line", (startLineNum+i));
			Job job=new Job(conf,"price caclute itetator="+(i+1));
			job.setJarByClass(CalAvgPrice.class);
			job.setMapperClass(CalAvgPriceMapper.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			
			FileInputFormat.setInputPaths(job,new Path(args[0]));
			FileOutputFormat.setOutputPath(job,new Path(args[1],"output_"+(i+1)));
			
			job.waitForCompletion(true);
		}
		System.exit(0);
	}

}
