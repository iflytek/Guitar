package com.iflytek.guitar.core.hpath;

import com.iflytek.guitar.core.alg.AlgDef;
import com.iflytek.guitar.core.util.ConstantsAlg;
import org.apache.hadoop.fs.Path;

import java.util.Calendar;
import java.util.Date;

public class PathDef {
    public static final String CONFIG_INTER = "report/config/%F/%Y%M%D%H/%ID";

    public String pathdef;

    public PathDef(String pathdef) {
        this.pathdef = pathdef;
    }

    public PathDef parseFreq(AlgDef.Frequence freq) {
        if (null != pathdef) {
            String ret = pathdef.replaceAll("%F", freq.toString());
            return new PathDef(ret);
        }
        return null;
    }

    public PathDef parseDate(Date dt) {
        if (null != pathdef) {
            Calendar cal = Calendar.getInstance();
            cal.setTime(dt);

            String strYear = String.format("%04d", cal.get(Calendar.YEAR));
            String strMonth = String.format("%02d", cal.get(Calendar.MONTH) + 1);
            String strDay = String.format("%02d", cal.get(Calendar.DAY_OF_MONTH));
            String strHour = String.format("%02d", cal.get(Calendar.HOUR_OF_DAY));

            String dateDir = pathdef.replaceAll(ConstantsAlg.YEAR_DATE_WORDCARD, strYear);
            dateDir = dateDir.replaceAll(ConstantsAlg.MONTH_DATE_WORDCARD, strMonth);
            dateDir = dateDir.replaceAll(ConstantsAlg.DAY_DATE_WORDCARD, strDay);
            dateDir = dateDir.replaceAll(ConstantsAlg.HOUR_DATE_WORDCARD, strHour);

            return new PathDef(dateDir);
        }
        return null;
    }

    public PathDef parseID(String id) {
        if (null != pathdef) {
            return new PathDef(pathdef.replaceAll("%ID", id));
        }
        return null;
    }

    public static PathDef get(String pdef) {
        return new PathDef(pdef);
    }

    @Override
    public String toString() {
        return this.pathdef;
    }

    public Path toPath() {
        return new Path(this.pathdef);
    }
}
