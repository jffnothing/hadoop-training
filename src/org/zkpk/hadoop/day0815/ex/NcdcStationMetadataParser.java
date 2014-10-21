package org.zkpk.hadoop.day0815.ex;

import org.apache.hadoop.io.Text;


public class NcdcStationMetadataParser {
  
  private String stationId;
  private String stationName;

  public boolean parse(String record) {
	  String [] arr=record.toString().split("\t",-1);
	  stationId=arr[0];
	  stationName=arr[1];
	  try{
		  Integer.parseInt(stationId);
		  return true;
	  }catch(NumberFormatException e){
		  return false;
	  }
  }
  
  public boolean parse(Text record) {
    return parse(record.toString());
  }
  
  public String getStationId() {
    return stationId;
  }

  public String getStationName() {
    return stationName;
  }
  
}
