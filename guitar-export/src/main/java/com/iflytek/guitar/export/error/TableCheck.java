package com.iflytek.guitar.export.error;

import com.iflytek.guitar.share.utils.Constants;
import com.iflytek.guitar.share.db.DBConnect;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class TableCheck
{
	private Map<String, FieldCheck> tableInfos = new HashMap<String, FieldCheck>();
	ExceptionDbLogWriter dbLog;
	private DBConnect dbConn;
	private String tableName;

	public boolean checkField(Map<String, String> mapFeilds)
	{
		try
		{
			for (Entry<String, FieldCheck> fieldInfo : tableInfos.entrySet())
			{
				String value = mapFeilds.get(fieldInfo.getKey());
				if (!fieldInfo.getValue().check(value))
				{
					if (null == dbLog)
					{
						dbLog = new ExceptionDbLogWriter(dbConn.getConnection());
					}

					int timestamp = mapFeilds.containsKey(Constants.FIELD_TIMESTAMP_NAME) ? Integer.parseInt(mapFeilds
							.get(Constants.FIELD_TIMESTAMP_NAME)) : (int) System.currentTimeMillis() / 1000;

					dbLog.write(tableName, fieldInfo.getKey(), "Unsuitable value: " + value, "da_platform", timestamp);
					return false;
				}
			}
		} catch (SQLException e)
		{
			return false;
		}

		return true;
	}

	/**
	 * Attention: this analysis is only suit for mysql database.
	 * 
	 * @throws SQLException
	 */
	public TableCheck(DBConnect dbConn, String tableName) throws SQLException
	{
		this.dbConn = dbConn;
		this.tableName = tableName;
		ResultSet rs = dbConn.exceQuerySelectSql("desc " + tableName);

		while (rs.next())
		{
			String field = rs.getString(1).toLowerCase();

			String type = rs.getString(2);
			String couldNull = rs.getString(3);
			String hasDefault = rs.getString(5);

			String extra = rs.getString(6);
			if (!"auto_increment".equalsIgnoreCase(extra))
			{
				FieldCheck check = getFieldInfo(type, couldNull, !"NULL".equalsIgnoreCase(hasDefault));
				tableInfos.put(field, check);
			}
		}
	}

	private FieldCheck getFieldInfo(String type, String couldNull, boolean hasDefault) throws SQLException
	{
		type = type.toLowerCase();
		String typeShort;
		int length = -1;

		int idxb = type.indexOf('(');
		if (idxb != -1)
		{
			int idxe = type.indexOf(')');
			assert (idxe != -1 && idxe > idxb + 1);
			typeShort = type.substring(0, idxb);
			String len = type.substring(idxb + 1, idxe);
			int idxlen = len.indexOf(',');
			if (idxlen != -1)
			{
				length = Integer.parseInt(len.substring(0, idxlen));
			} else
			{
				length = Integer.parseInt(len);
			}
		} else
		{
			typeShort = type;
		}

		return getFieldInfo(typeShort, length, "YES".equalsIgnoreCase(couldNull), hasDefault);
	}

	private FieldCheck getFieldInfo(String type, int length, boolean cldNull, boolean hasDefault)
	{
		FieldCheck check;
		if ("tinyint".equals(type) || "smallint".equals(type))
		{
			check = new MysqlNumCheck(cldNull, hasDefault);
		} else if ("int".equals(type) || "mediumint".equals(type))
		{
			check = new MysqlNumCheck(cldNull, hasDefault);
		} else if ("bigint".equals(type))
		{
			check = new MysqlNumCheck(cldNull, hasDefault);
		} else if ("varchar".equals(type) || "char".equals(type))
		{
			check = new MysqlStrCheck(cldNull, hasDefault, length);
		} else if ("float".equals(type))
		{
			check = new MysqlNumCheck(cldNull, hasDefault);
		} else if ("double".equals(type) || "decimal".equals(type))
		{
			check = new MysqlNumCheck(cldNull, hasDefault);
		} else if ("tinytext".equals(type))
		{
			check = new MysqlStrCheck(cldNull, hasDefault, 255);
		} else if ("text".equals(type))
		{
			check = new MysqlStrCheck(cldNull, hasDefault, 65535);
		} else if ("mediumtext".equals(type) || "longtext".equals(type))
		{
			check = new MysqlStrCheck(cldNull, hasDefault, length);
		} else if ("date".equals(type))
		{
			check = new MysqlNumCheck(cldNull, hasDefault);
		} else if ("time".equals(type))
		{
			check = new MysqlNumCheck(cldNull, hasDefault);
		} else if ("timestamp".equals(type))
		{
			check = new MysqlNumCheck(cldNull, hasDefault);
		} else
		{
			check = new MysqlNumCheck(cldNull, hasDefault);
		}

		return check;
	}

	class MysqlNumCheck implements FieldCheck
	{
		private boolean cldNull;
		private boolean hasDefault;

		public MysqlNumCheck(boolean colNull, boolean hasDefault)
		{
			this.cldNull = colNull;
			this.hasDefault = hasDefault;
		}

		@Override
		public boolean check(Object obj)
		{
			if (!cldNull && obj == null && !hasDefault)
			{
				return false;
			}
			return true;
		}

		@Override
		public int getLenMax()
		{
			return -1; //数字的长度暂时不检查
		}
	}

	class MysqlStrCheck implements FieldCheck
	{
		private boolean cldNull;
		private boolean hasDefault;
		private int len = -1;

		public MysqlStrCheck(boolean cldNull, boolean hasDefault, int len)
		{
			this.cldNull = cldNull;
			this.hasDefault = hasDefault;
			this.len = len;
		}

		@Override
		public boolean check(Object obj)
		{
			if ((!cldNull && obj == null && !hasDefault)
					|| ((len != -1) && (null != obj) && (obj.toString()).getBytes().length > len))
			{
				return false;
			}
			return true;
		}

		@Override
		public int getLenMax()
		{
			return len;
		}

	}
}
