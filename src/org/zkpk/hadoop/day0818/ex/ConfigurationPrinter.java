package org.zkpk.hadoop.day0818.ex;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConfigurationPrinter extends Configured implements Tool {
//	static{
//		Configuration.addDefaultResource("configuration-1.xml");
//		Configuration.addDefaultResource("configuration-2.xml");
//	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		conf.addResource("configuration-1.xml");
		conf.addResource("configuration-2.xml");
		for(Entry<String,String> entry:conf){
			System.out.printf("%s=%s\n",entry.getKey(),entry.getValue());
		}
		//System.out.print(conf.get("color"));
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new ConfigurationPrinter(), args);
		System.exit(exitCode);
	}

}
