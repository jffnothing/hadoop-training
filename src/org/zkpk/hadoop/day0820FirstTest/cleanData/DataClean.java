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
//		bw.write("农产品品类"+"\t"+"2014年1月6日"+"\t"+"2014年1月7日"+"\t"+"2014年1月8日"+"\t"+"2014年1月9日"+"\t"+"2014年1月10日"+"\t"+"批发市场名称");
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
				if(arr.length==7){//最后26-31的数据是六天的数据，而非七天的数据
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
