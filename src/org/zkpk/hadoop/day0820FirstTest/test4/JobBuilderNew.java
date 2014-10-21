package org.zkpk.hadoop.day0820FirstTest.test4;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class JobBuilderNew {
	
	public static Job parseInputAndOutput(String str,String[] args) throws IOException {
		
		if(args.length!=2){
			printUsage("<input> <output>");
			return null;
		}
		Job job=new Job(new Configuration(),str);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job;
	}

	private static void printUsage( String extraArgsUsage) {
		// TODO Auto-generated method stub
		System.err.printf("Usage: %s [genericOption] %s\n\n",extraArgsUsage);
		GenericOptionsParser.printGenericCommandUsage(System.err);
	}
}
