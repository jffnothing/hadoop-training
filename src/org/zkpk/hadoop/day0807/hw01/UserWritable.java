package org.zkpk.hadoop.day0807.hw01;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class UserWritable implements WritableComparable<UserWritable>{
	private Text uid;
	private Text name;
	private IntWritable age;
	private Text sex;
	
	public UserWritable(){
		set(new Text(),new Text(),new IntWritable(),new Text());
	}
	public UserWritable(Text uid,Text name,IntWritable age,Text sex){
		set(uid,name,age,sex);
	}
	public UserWritable(String uid,String name,int age,String sex){
		set(new Text(uid),new Text(name),new IntWritable(age),new Text(sex));
	}
	
	private void set(Text uid, Text name, IntWritable age,Text sex) {
		this.uid=uid;
		this.name=name;
		this.age=age;
		this.sex=sex;
	}
	
	public Text getId(){
		return uid;
	}
	public Text getName(){
		return name;
	}
	public IntWritable getAge(){
		return age;
	}
	public Text getSex(){
		return sex;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		uid.write(out);
		name.write(out);
		age.write(out);
		sex.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		uid.readFields(in);
		name.readFields(in);
		age.readFields(in);
		sex.readFields(in);
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return uid.hashCode()*163+name.hashCode()+age.hashCode()+sex.hashCode();
	}
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if(obj instanceof UserWritable){
			UserWritable uw=(UserWritable) obj;
			return uid.equals(uw.uid) && name.equals(uw.name) && age.equals(uw) && sex.equals(uw);
		}
		return super.equals(obj);
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return	uid+"\t"+name+"\t"+age+"\t"+sex;
	}
	@Override
	public int compareTo(UserWritable o) {
		// TODO Auto-generated method stub
		int cmp1=uid.compareTo(o.uid);
		if(cmp1!=0){
			return cmp1;
		}
		int cmp2=name.compareTo(o.name);
		if(cmp2!=0){
			return cmp2;
		}
		int cmp3=age.compareTo(o.age);
		if(cmp3!=0){
			return cmp3;
		}
		return sex.compareTo(o.sex);
	}
	public static void main(String [] args){
		UserWritable uw1=new UserWritable("01","zhangsan",20,"ÄÐ");
		UserWritable uw2=new UserWritable("03","lisi",20,"Å®");
		System.out.println(uw1.compareTo(uw2));
		System.out.println(uw1.equals(uw2));
		System.out.println(uw1.toString());
	}
}
