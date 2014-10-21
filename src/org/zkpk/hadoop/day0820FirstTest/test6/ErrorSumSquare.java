package org.zkpk.hadoop.day0820FirstTest.test6;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ErrorSumSquare extends Configured implements Tool{

	static class ErrorSumSquareMapper extends Mapper<Object,Text,DoubleWritable,NullWritable>{
		private Map<String, Double> joinData = new HashMap<String, Double>();
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if(cacheFiles != null && cacheFiles.length > 0){
				String line;
				String[] tokens;
				for(int i=0;i<cacheFiles.length;i++){
					BufferedReader joinReader = new BufferedReader(new FileReader(cacheFiles[i].toString()));
					while ((line = joinReader.readLine()) != null) {
						tokens = line.split("\t", -1);
						double calprice=Double.parseDouble(tokens[1]);
						joinData.put(tokens[0], calprice);
					}
					joinReader.close();
				}	
			}
		}	
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
				String [] arr=value.toString().split("\t");
				if(arr.length==6){
					double day4realprice=Double.parseDouble(arr[4]);
					double day4calprice=joinData.get("2014/1/4");
					double day4square=(day4realprice-day4calprice)*(day4realprice-day4calprice);
					day4square=Math.ceil(day4square*10+0.5)/10;
					
					
					double day5realprice=Double.parseDouble(arr[5]);
					double day5calprice=joinData.get("2014/1/5");
					double day5square=(day5realprice-day5calprice)*(day5realprice-day5calprice);
					day5square=Math.ceil(day5square*10+0.5)/10;
					
					double ess=day4square+day5square;
					
					context.write(new DoubleWritable(ess), NullWritable.get());
				}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		DistributedCache.addCacheFile(new Path("/day0820firstTest/test6_2/output_1/part-r-00000").toUri(), conf);
		DistributedCache.addCacheFile(new Path("/day0820firstTest/test6_2/output_2/part-r-00000").toUri(), conf);
		//hdfs中的文件
		Job job = new Job(conf);
		job.setJobName("MapJoin with DistributedCache");
		job.setJarByClass(ErrorSumSquare.class);
		job.setMapperClass(ErrorSumSquareMapper.class);
		job.setNumReduceTasks(0);
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new ErrorSumSquare(), args);
		System.exit(res);
	}

}
