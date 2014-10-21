package org.zkpk.hadoop.day0901.ex;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class ZipFileTest {

	public static void main(String[] args) throws Exception {
		read();
	}

	public static String unZip(byte[] data) {
		
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			ZipInputStream zip = new ZipInputStream(bis);
			BufferedReader br=new BufferedReader(new InputStreamReader(zip));
			while (zip.getNextEntry() != null) {
				String line="";
				while((line=br.readLine())!=null){
					System.out.println(line);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return null;
	}

	public static void read() throws Exception {
		File f = new File("D:\\1.zip");
		byte[] buff = new byte[1024];
		FileInputStream fi = new FileInputStream(f);
		fi.read(buff);
		unZip(buff);
	}

}
