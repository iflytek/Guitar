package com.iflytek.guitar.share.utils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public final class SafeDate {

    public static synchronized Date Format2Date(String strDate, String format) throws IOException {
        Date ret = null;
        try {
            ret = (new SimpleDateFormat(format)).parse(strDate);
        } catch (ParseException ex) {
            throw new IOException("Date parse error: " + strDate + " by fromat " + format);
        }

        return ret;
    }

    public static synchronized String Date2Format(Date date, String format) {
        return (new SimpleDateFormat(format)).format(date);
    }

}
