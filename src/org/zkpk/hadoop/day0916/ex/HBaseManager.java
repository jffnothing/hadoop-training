package org.zkpk.hadoop.day0916.ex;



import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseManager extends Thread {
	public Configuration config;
	public HTable table;
	public HBaseAdmin admin;
	
	public HBaseManager() {
		config = HBaseConfiguration.create();
		config.set("hbase.master", "master:60000");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.zookeeper.quorum","master");
		try {
			table = new HTable(config, Bytes.toBytes("sogou2"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void createTable() throws IOException{
		HTableDescriptor tableDesc = new HTableDescriptor("sogou1");
		tableDesc.addFamily(new HColumnDescriptor("s1"));
		admin.createTable(tableDesc);
	}
	public void deleteTable() throws IOException{
		String tablename = "sogou_mark";
		admin.disableTable(tablename);
		admin.deleteTable(tablename);
	}
	public void insert() throws IOException{
		Put put = put = new Put(Bytes.toBytes("row1"));
		put.add(
				Bytes.toBytes("cf1"), 
				Bytes.toBytes("id"),
				Bytes.toBytes("6"));
		put.add(Bytes.toBytes("cf1"), 
				Bytes.toBytes("name"),
				Bytes.toBytes("zs"));
		table.put(put);
	}
	public void read() throws IOException{
		long st=System.currentTimeMillis();
		Get get = new Get(Bytes.toBytes("04edb46843c48043a413a910c5b4ca7c"));
		//get.addFamily(Bytes.toBytes("s1"));
		get.addColumn(Bytes.toBytes("s1"), Bytes.toBytes("time"));
		get.setMaxVersions();    //max get two version
		Result dbResult = table.get(get);
//		System.out.println(
//				"size=" + dbResult.size() +
//				", value1=" +
//				Bytes.toString(dbResult.list().get(0).getValue()) //+
//				",value2=" +
//				Bytes.toString(dbResult.list().get(1).getValue()) +
//				",value3=" +
//				Bytes.toString(dbResult.list().get(2).getValue()) 
				
//		);
		long et=System.currentTimeMillis();
		System.out.println("get---time:"+(et-st)+"...ms");
		
	}
	public void scan() throws IOException{
		long st=System.currentTimeMillis();
		Scan scanner=new Scan();
		scanner.setTimeRange(0L,Long.MAX_VALUE);
		
		String [] columns="s1:time".split(",",-1);
		for(String col:columns){
			byte[][] colkey=KeyValue.parseColumn(Bytes.toBytes(col));//parseColumn split string use ","
			if(colkey.length>1){
				scanner.addColumn(colkey[0],colkey[1]);
			}else{
				scanner.addFamily(colkey[0]);
			}
		}
		scanner.setMaxVersions();
		scanner.setBatch(0);
		scanner.setCaching(100000);
		ResultScanner rsScanner=table.getScanner(scanner);
		for(Result res:rsScanner){
			final List<KeyValue> list=res.list();
			for(final KeyValue kv:list){
				//System.out.println("rowkey="+Bytes.toString(res.getRow())+"ts="+kv.getTimestamp()+",value="+Bytes.toString(kv.getValue()));
			}
		}
		long et=System.currentTimeMillis();
		System.out.println("scan---time:"+(et-st)+"...ms");
		rsScanner.close();
	}
	public void delete() throws IOException{
		Delete del=new Delete(Bytes.toBytes("row1"));
		del.deleteColumn(Bytes.toBytes("cf1"),Bytes.toBytes("id"));
		del.deleteColumns(Bytes.toBytes("cf1"),Bytes.toBytes("id"));
		del.deleteFamily(Bytes.toBytes("cf1")); // tombstones's time is currunt time
		table.delete(del);
	}
	public static final byte[] POSTFIX=new byte[]{1};
	public void testPagefilter() throws IOException{
		//1
		Filter filter = new PageFilter(5);
		int totalRows = 0;
		byte[] lastRow = null;
		//byte[] POSTFILE=new byte[100000];
		//2
		while (true) {
		//3
		Scan scan = new Scan();
		//4
		scan.setFilter(filter);
		//5
		if (lastRow != null) {
			byte[] startRow = Bytes.add(lastRow, POSTFIX);
			System.out.println("start row: "
					+ Bytes.toStringBinary(startRow));
			scan.setStartRow(startRow);
		}
		//6
		ResultScanner scanner = table.getScanner(scan);
		int localRows = 0;
		Result result;
		while ((result = scanner.next()) != null) {
			System.out.println(localRows++ + ": " + result);
			totalRows++;
			//localRows++;
			lastRow = result.getRow();
		}
		//7
		scanner.close();
		//8
		if (localRows == 0)
			break;
		}
		System.out.println("total rows: " + totalRows);
	}
	public void selectresult1() throws IOException{
		Scan scan=new Scan();
		String [] columns="cf1:id,cf1:name".split(",",-1);
		for(String col:columns){
			byte[][] colkey=KeyValue.parseColumn(Bytes.toBytes(col));//parseColumn split string use ","
			if(colkey.length>1){
				scan.addColumn(colkey[0],colkey[1]);
			}else{
				scan.addFamily(colkey[0]);
			}
		}
		scan.setMaxVersions();
		scan.setBatch(0);
		scan.setCaching(100000);
		ResultScanner rs=table.getScanner(scan);
		for(Result r:rs){
			KeyValue [] kvs=r.raw();
			System.out.println(Bytes.toString(r.getRow())+new String(kvs[3].getValue()));
			
			final List<KeyValue> list=r.list();
			for(final KeyValue kv:list){
				System.out.println("rowkey="+Bytes.toString(r.getRow())+",value="+Bytes.toString(kv.getValue()));
			}
		}
		rs.close();
	}
	public static void main(String [] args) throws IOException{
		HBaseManager hbm=new HBaseManager();
		//hbm.createTable();
		//hbm.deleteTable();
		//hbm.insert();
		//hbm.read();
		//hbm.scan();
		//hbm.delete();
		//hbm.scan();
		//hbm.selectresult1();
		//hbm.testPagefilter();
	}	
}
