package com.iflytek.guitar.export.error;

public class ToDbUtils {
	
	public static String getSubStringByte(String str, int maxByteLen){
		if (null!=str && maxByteLen>-1 && str.getBytes().length > maxByteLen){
			byte[] tmp = new byte[maxByteLen];
			for(int i=0; i<maxByteLen; ++i){
				tmp[i] = str.getBytes()[i];
			}
			return new String(tmp);
		}else{
			return str;
		}
	}
	
	public static long numRecordErr = 0;
}
