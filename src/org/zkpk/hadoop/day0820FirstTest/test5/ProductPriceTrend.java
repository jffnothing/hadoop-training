package org.zkpk.hadoop.day0820FirstTest.test5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProductPriceTrend {
	public static void main(String args[]) throws Exception {
		Job job = new Job();
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(ProductPriceTrend.class);
		job.setMapperClass(Test5Mapper.class);
		job.setReducerClass(Test5Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.waitForCompletion(true);
	}
}

class Test5Mapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	private String year;
	private Text outKey = new Text();
	private FloatWritable outValue = new FloatWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		String[] cols=line.split("\t");
		System.out.println(cols.length+"....len");
		if(line.contains("农产品分类")){
			year=cols[1].trim();
		}else if(cols.length==5&&cols[3].contains("山西")){
			String k=cols[0]+"_"+year;
			outKey.set(k);
			float price=Float.parseFloat(cols[1]);
			System.out.println(k+price);
			outValue.set(price);
			context.write(outKey, outValue);
			
		}
		
	}
}

class Test5Reducer extends Reducer<Text, FloatWritable, Text, Text> {
	private Text outKey=new Text();
	private Text outValue=new Text();
	private List<Float> prices=new ArrayList<Float>();
	private TreeMap<String,Float> maps=new TreeMap<String,Float>();
	//private TreeMap<String,List<Float>> maps=new TreeMap<String,List<Float>>();
	@Override
	protected void reduce(Text arg0, Iterable<FloatWritable> arg1, Context arg2)
			throws IOException, InterruptedException {
		for(FloatWritable fw:arg1){
			prices.add(fw.get());
		}
		float avg=getAvg(prices);
		maps.put(arg0.toString(), avg);
	}
	
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		String times="时间:";
		Set<String> keySet=maps.keySet();
		int i=0;
		for(String k:keySet){
			i++;
			times+="\t\t"+k.split("_")[1];
			if(i%5==0){
				break;
			}
		}
		outKey.set(times);
		context.write(outKey, outValue);
		String prs="";
		for(String k:keySet){
			prs+="\t\t"+maps.get(k);
			i++;
			if(i%5==0){
				outKey.set(k.split("_")[0]);
				outValue.set(prs);
				context.write(outKey, outValue);
				prs="";
			}
		}
	}
	
	private float getAvg(List<Float> prices){
		if(prices!=null&&prices.size()>0){
			if(prices.size()==1){
				return prices.get(0);
			}else if(prices.size()==2){
				return (prices.get(0)+prices.get(1))/2;
			}else{
				removeMaxAndMin(prices);
				float avg=0f;
				float sum=0f;
				for(Float p:prices){
					sum+=p;
				}
				avg=sum/prices.size();
				return avg;
			}
		}
		return 0;
	}

	private void removeMaxAndMin(List<Float> prices) {
		float max=prices.get(0);
		float min=prices.get(0);
		for(Float p:prices){
			if(p>max){
				max=p;
			}else if(p<min){
				min=p;
			}
		}
		prices.remove(max);
		prices.remove(min);
	}
}


