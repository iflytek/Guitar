package com.iflytek.guitar.share.utils;

public interface Constants {

    public final static String GUITAR_BASE_DIR = "iflytek.guitar.base.dir";

    public final static String FIELD_TIMESTAMP_NAME = "timestamp";

    public final static String DATE_FORMAT_BASIC = "yyyy-MM-dd'T'HH:mm'Z'";
    public final static String DATE_FORMAT_DAILY_DIR = "yyyy-MM-dd";
    public final static String DATE_FORMAT_HOURLY_DIR = "yyyy-MM-dd/HH";
    public final static String DATE_FORMAT_HOURLY = "yyyyMMdd-HH";
    public final static String DATE_FORMAT_MSEC = "yyyyMMdd-HHmmss-S";

    public static final String VALUE_DATA_CHECK_NO = "no";
    public static final String VALUE_DATA_CHECK_PART = "part";
    public static final String VALUE_DATA_CHECK_ALL = "all";

    public final static String TARGET_FEILD_ANATYPE_SUM = "SUM";
    public final static String TARGET_FEILD_ANATYPE_MAX = "MAX";
    public final static String TARGET_FEILD_ANATYPE_MIN = "MIN";
    public final static String TARGET_FEILD_ANATYPE_AVG = "AVG";
    public final static String TARGET_FEILD_ANATYPE_VALUEDIST = "VALUEDIST";
    public final static String TARGET_FEILD_ANATYPE_RANGEDIST = "RANGEDIST";
    public final static String TARGET_FEILD_ANATYPE_UVBITMAP = "UVBITMAP";
    public final static String TARGET_FEILD_ANATYPE_UV_HYPERLOGLOG = "UV_HYPERLOGLOG";
    public final static String TARGET_FEILD_ANATYPE_UV_HYPERLOGLOGPLUS = "UV_HYPERLOGLOGPLUS";
    public final static String TARGET_FEILD_ANATYPE_UV = "UV";

    public final static String FREQ_HOURLY = "Hourly";
    public final static String FREQ_DAILY = "Daily";
    public final static String FREQ_WEEKLY = "Weekly";
    public final static String FREQ_7DAYS = "7days";
    public final static String FREQ_MONTHLY = "Monthly";
    public final static String FREQ_30DAYS = "Thirtydays";
    public final static String FREQ_QUARTERLY = "Quarterly";
    public final static String FREQ_HALFYEARLY = "HalfYearly";
    public final static String FREQ_YEARLY = "Yearly";

    public final static String DIR_REPORT_DIR = "report";
    public final static String DIR_REPORT_SUBDIR_INTERM = "interm";
    public final static String DIR_REPORT_SUBDIR_OUTPUT = "output";
    public final static String DIR_REPORT_SUBDIR_TMP = "tmp";
    public final static String DIR_REPORT_DIR_CURRENT = "current";
    public final static String FILENAME_SUCCESS = "_SUCCESS";

    public final static int RET_INT_ERROR_NORD = -2; // no report data input error
    public final static int RET_INT_ERROR = -1;
    public final static int RET_INT_OK = 0;

    public final static Boolean IS_TEST = UtilOper.isTest();

}
