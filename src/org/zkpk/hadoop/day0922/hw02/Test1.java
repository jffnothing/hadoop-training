package org.zkpk.hadoop.day0922.hw02;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class Test1 {
	public HTable table;
	public Configuration config;
	public HBaseAdmin admin;
	/**
	 * @param args
	 * @throws IOException 
	 */
	public Test1() throws IOException{
		config=HBaseConfiguration.create();
		config.set("hbase.master","master");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.zookeeper.quorum","master");
		table=new HTable(config,Bytes.toBytes("counters"));
		admin=new HBaseAdmin(config);
	}
	public void testIncr() throws IOException{
		long st=System.currentTimeMillis();
		byte[] rk=Bytes.toBytes("rk1");
		byte[] cf=Bytes.toBytes("cf1");
		byte[] c=Bytes.toBytes("count");
		
		long cur=table.incrementColumnValue(rk, cf,c,0L);
		System.out.println("cur:"+cur);
		cur=table.incrementColumnValue(rk, cf, c, 1024L);
		System.out.println("cur:"+cur);
		cur=table.incrementColumnValue(rk, cf, c, 1L);
		System.out.println("cur:"+cur);
		
		long en=System.currentTimeMillis();
		System.out.println("time:"+(en-st)+"...ms");
		
	}
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Test1 test1=new Test1();
		test1.testIncr();
	}
	/*
	 * 
	 * ERROR: org.apache.hadoop.hbase.DoNotRetryIOException: org.apache.hadoop.hbase.DoNotRetryIOException: Attempted to increment field that isn't 64 bits wide
	 * 
	 * */
}
