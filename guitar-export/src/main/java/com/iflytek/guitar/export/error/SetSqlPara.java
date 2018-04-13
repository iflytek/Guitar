package com.iflytek.guitar.export.error;

import java.sql.SQLException;

public interface SetSqlPara {
	void setValue(int paraIndex, Object value) throws SQLException;

}
