package org.zkpk.hadoop.day0902;

import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

public class TestHiveServer {
	
	public static void main(String[] args) throws HiveServerException, TException{
		TSocket transport = new TSocket("192.168.32.128",10001);
		TBinaryProtocol protocol = new TBinaryProtocol(transport);
		
		HiveClient client = new HiveClient(protocol);
		transport.open();
		client.execute("select * from sogou.sogou limit 10");
		System.out.println(client.fetchOne());	
	}
}
