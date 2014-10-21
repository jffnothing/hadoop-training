package org.zkpk.hadoop.day0901.ex;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name="zodiac",
			 value="_FUNC_(date)-from the input date string"+
					 "or separate month and day arguments,returns the sign of the Zodiac.",
			 extended="Example:\n")
public class UDFZodiacSign extends UDF {
	private SimpleDateFormat df;
	public UDFZodiacSign(){
		df=new SimpleDateFormat("MM-dd-yyyy");
	}
	public String evaluate(Date bday){
		GregorianCalendar cal = new GregorianCalendar(); 
		cal.setTime(bday);
		//return this.evaluate(cal.MONTH,cal.DAY_OF_MONTH);
		int month=cal.get(GregorianCalendar.MONTH);
		int day=cal.get(GregorianCalendar.DAY_OF_MONTH);
		return this.evaluate(new Integer(month+1),new Integer(day));
	}
	private String evaluate(Integer month, Integer day) {
		// TODO Auto-generated method stub
		if(month==7){
			if(day<23){
				return "¾ÞÐ·×ù";
			}else{
				return "Ê¨×Ó×ù";
			}
		}
		if(month==9){
			if(day<23){
				return "´¦Å®×ù";
			}else{
				return "Ìì³Ó×ù";
			}
		}
		if(month==11){
			if(day<23){
				return "ÌìÐ«×ù";
			}else{
				return "ÉäÊÖ×ù";
			}
		}
		return "Ê¨×Ó×ù";
	}
	public String evaluate(String bday){
		Date date=null;
		try {
			date=df.parse(bday);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			return null;
		}
		GregorianCalendar cal = new GregorianCalendar(); 
		cal.setTime(date);
		int month=cal.get(GregorianCalendar.MONTH);
		int day=cal.get(GregorianCalendar.DAY_OF_MONTH);
//		System.out.println(month);
//		System.out.println(day);
		return this.evaluate(new Integer(month+1),new Integer(day));
	}
//	public static void main(String [] args){
//		UDFZodiacSign test=new UDFZodiacSign();
//		System.out.println(test.evaluate("11-28-1992"));
//	}
}
