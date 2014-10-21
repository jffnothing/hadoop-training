package org.zkpk.hadoop.day0820FirstTest.test6;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CukeAvgPriceTrend extends Configured implements Tool {
	static class CukeAvgPriceMapper extends Mapper<Object,Text,Text,Text>{
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] arr=value.toString().split("\t");
			if(arr.length==9){
				String province=arr[7];
				String name=arr[0];
				String price=arr[1]+"\t"+arr[2]+"\t"+arr[3]+"\t"+arr[4]+"\t"+arr[5];
				if(province.equals("ɽ��")&&name.equals("�ƹ�")){
					context.write(new Text(name),new Text(price));
				}
			}
		}	
	}
	
	static class CukeAvgPriceReducer extends Reducer<Text,Text,Text,Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			//��ǰũ��Ʒ���������г��е�1-5�յļ۸�
			ArrayList<Double> day1=new ArrayList();
			ArrayList<Double> day2=new ArrayList();
			ArrayList<Double> day3=new ArrayList();
			ArrayList<Double> day4=new ArrayList();
			ArrayList<Double> day5=new ArrayList();
			//����ļ۸�ŵ�һ��list��
			ArrayList<ArrayList<Double>> day=new ArrayList();
			
			for(Text value:values){
				String [] arr=value.toString().split("\t");
				if(arr.length==5){
					day1.add(Double.parseDouble(arr[0]));
					day2.add(Double.parseDouble(arr[1]));
					day3.add(Double.parseDouble(arr[2]));
					day4.add(Double.parseDouble(arr[3]));
					day5.add(Double.parseDouble(arr[4]));
				}	
			}
			
			day.add(day1);
			day.add(day2);
			day.add(day2);
			day.add(day4);
			day.add(day5);
			//��ÿ��ļ۸��������
			for(int n=0;n<day.size();n++){
				for(int i=day.get(n).size()-1;i>=0;i--){
					for(int j=0;j<i;j++){
						if(day.get(n).get(j)>day.get(n).get(j+1)){
							double term=day.get(n).get(j);
							day.get(n).set(j, day.get(n).get(j+1));
							day.get(n).set(j+1,term);
						}
					}
				}
			}
			ArrayList<Double> price=new ArrayList();
			//��ÿ���ƽ��ֵ
			for(int n=0;n<day.size();n++){
				double sum=0;
				for(int i=0;i<day.get(n).size();i++){
					sum+=day.get(n).get(i);
				}
				double avg=(sum-day.get(n).get(0)-day.get(n).get(day.get(n).size()-1))/(day.get(n).size()-2);
				double num=Math.ceil(avg*10+0.5)/10;
				price.add(num);
			}
			String perPrice="";
			for(int n=0;n<price.size();n++){
				perPrice=perPrice+"\t"+price.get(n);
			}
			//���ÿ���ƽ���۸�
			context.write(key,new Text(perPrice));
		}	
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=NewJobBuilder.parseInputAndOutput(this, getConf(), args);
		job.setJarByClass(CukeAvgPriceTrend.class);
		job.setMapperClass(CukeAvgPriceMapper.class);
		job.setReducerClass(CukeAvgPriceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true)?0:1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new CukeAvgPriceTrend(),args);
		System.exit(exitCode);
	}

}
