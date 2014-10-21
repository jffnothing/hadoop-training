package org.zkpk.hadoop.day0820FirstTest.cleanData;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class DataClean {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader br=new BufferedReader(new FileReader("d:\\26-31.txt"));
		BufferedWriter bw=new BufferedWriter(new FileWriter("d:\\cleandata.txt"));
//		bw.write("ũ��ƷƷ��"+"\t"+"2014��1��6��"+"\t"+"2014��1��7��"+"\t"+"2014��1��8��"+"\t"+"2014��1��9��"+"\t"+"2014��1��10��"+"\t"+"�����г�����");
//		bw.newLine();
		String line=br.readLine();
		String market="";
		//String province="";
		//String city="";
		while(line!=null){
			if(!line.equals("")){
				String [] arr=line.toString().split("\t");
				if(arr.length==1&&arr[0].length()>6){
					market=arr[0];
					for(int i=0;i<3;i++){
						line=br.readLine();
					}
				}
				if(arr.length==7){//���26-31����������������ݣ��������������
					String record=line+"\t"+market;
					bw.write(record);
					bw.newLine();
				}
				line=br.readLine();
			}
			else{
				line=br.readLine();
			}
			
		}
		bw.flush();
		br.close();	
		bw.close();
		
	}

}
