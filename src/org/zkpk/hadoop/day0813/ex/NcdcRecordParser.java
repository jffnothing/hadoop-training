package org.zkpk.hadoop.day0813.ex;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {
	private static final int MISSING_TEMPERATURE=9999;
	private String year;
	private int airTemperature;
	private String quality;
	private String stationId;
	
	public void parse(String record){
		year=record.substring(15,19);
		String airTemperatureString;
		if(record.charAt(87)=='+'){
			airTemperatureString=record.substring(88,92);
		}else{
			airTemperatureString=record.substring(87,92);
		}
		airTemperature=Integer.parseInt(airTemperatureString);
		quality=record.substring(92,93);
		stationId=record.substring(4,15);
	}
	public void parse(Text record){
		parse(record.toString());
	}
	public boolean isValidTemperature(){
		return airTemperature!=MISSING_TEMPERATURE&&quality.matches("[01459]");
	}
	public boolean isMalformedTemperature(){
		return quality.matches("[01459]");
	}
	public boolean isMissingTemperature(){
		return airTemperature!=MISSING_TEMPERATURE;
	}
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
	public String getQuality() {
		return quality;
	}
	public void setQuality(String quality) {
		this.quality = quality;
	}
	public void setAirTemperature(int airTemperature) {
		this.airTemperature = airTemperature;
	}
	public void setStationId(String stationId) {
		this.stationId = stationId;
	}
	public String getyear(){
		return year;
	}
	public int getAirTemperature(){
		return airTemperature;
	}
	public String getStationId() {
		// TODO Auto-generated method stub
		return stationId;
	}
}
