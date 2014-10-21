package org.zkpk.hadoop.day0922.hw02;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class Test2 {
		public HTable table;
		public Configuration config;
		public HBaseAdmin admin;
		public Test2() throws IOException{
			config=HBaseConfiguration.create();
			config.set("hbase.master","master");
			config.set("hbase.zookeeper.property.clientPort", "2181");
			config.set("hbase.zookeeper.quorum","master");
			table=new HTable(config,Bytes.toBytes("counters"));
			admin=new HBaseAdmin(config);
		}
		public void MutiIncr() throws IOException{
			Increment incr=new Increment(Bytes.toBytes("rk2"));
			incr.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("count1"),1);
			incr.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("count2"),1);
			incr.addColumn(Bytes.toBytes("cf2"),Bytes.toBytes("count1"),10);
			incr.addColumn(Bytes.toBytes("cf2"),Bytes.toBytes("count2"),10);
			
			Result result=table.increment(incr);
			for(KeyValue kv:result.raw()){
				System.out.println("KV:"+kv+"value:"+Bytes.toLong(kv.getValue()));
			}
		}
		public static void main(String[] args) throws IOException {
			// TODO Auto-generated method stub
			Test2 test2=new Test2();
			test2.MutiIncr();
		}

}
