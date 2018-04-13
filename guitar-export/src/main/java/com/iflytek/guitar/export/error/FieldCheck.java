package com.iflytek.guitar.export.error;

public interface FieldCheck
{
	boolean check(Object obj);
	
	int getLenMax();
}
