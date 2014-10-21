package org.zkpk.hadoop.day0901.ex;

import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

public class TestHiveServer {
	
	public static void main(String[] args) throws HiveServerException, TException {
		// TODO Auto-generated method stub
		//start ContOS's hive service 
		//bin/hive --server hiveserver &
		//netstat -nl | grep 10000
		//import hive jar 
			TSocket transport=new TSocket("192.168.32.128",10000);
			TBinaryProtocol protocol=new TBinaryProtocol(transport);
			HiveClient client=new HiveClient(protocol);
			transport.open();
			client.execute("show tables");
			System.out.println(client.fetchAll());
	}

}
