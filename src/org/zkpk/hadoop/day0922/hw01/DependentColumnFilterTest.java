package org.zkpk.hadoop.day0922.hw01;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class DependentColumnFilterTest {

	/**
	 * @param args
	 */
	public Configuration config;
	public static HTable table;
	public HBaseAdmin admin;
	
	public DependentColumnFilterTest() {
		config = HBaseConfiguration.create();
		config.set("hbase.master", "master:60000");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.zookeeper.quorum","master");
		try {
			table = new HTable(config, Bytes.toBytes("test"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		DependentColumnFilterTest scft=new DependentColumnFilterTest();
		Scan scan=new Scan();
		scan.setMaxVersions();
		
		//byte[][] colkey=KeyValue.parseColumn(Bytes.toBytes("row1:name"));
		
		FilterList filter=new FilterList();
		
//		DependentColumnFilter dependentFilter=new DependentColumnFilter
//		(Bytes.toBytes("cf1"),Bytes.toBytes("name"),true,
//				CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("001")));
		SingleColumnValueExcludeFilter singFilter=new SingleColumnValueExcludeFilter
		(Bytes.toBytes("cf1"),Bytes.toBytes("uid"),CompareOp.EQUAL,new RegexStringComparator("001"));
		
		
		filter.addFilter(singFilter);
		
		scan.setFilter(filter);
		ResultScanner rsScanner=table.getScanner(scan);
		for(Result rs:rsScanner){
			 for (KeyValue kv : rs.list()) {
                System.out.println("kv=" + kv.toString() + ",value="
                        + Bytes.toString(kv.getValue()));
            }
		}
	}

}
