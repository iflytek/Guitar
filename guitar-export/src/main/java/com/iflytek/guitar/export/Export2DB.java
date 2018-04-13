package com.iflytek.guitar.export;

import com.iflytek.guitar.export.error.TableCheck;
import com.iflytek.guitar.share.utils.Constants;
import com.iflytek.guitar.share.db.DBConnect;
import com.iflytek.guitar.share.db.Exportable;
import com.iflytek.guitar.share.avro.io.Pair;
import com.iflytek.guitar.share.avro.mapreduce.FsInput;
import com.iflytek.guitar.share.avro.util.AvroUtils;
import org.apache.avro.file.DataFileReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import java.io.IOException;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Export2DB
{
	private static final Log log = LogFactory.getLog(Export2DB.class);
	private static final int DB_COMMIT_TIMES = 1;
	
	protected Map<String, DataFileReader<Object>> getReaders(Path dir, FileSystem fs, Configuration conf)
			throws IOException
	{
		Path[] aFilePath = FileUtil.stat2Paths(fs.listStatus(dir));
		Arrays.sort(aFilePath);
		
		Map<String, DataFileReader<Object>> mapReader = Maps.newHashMap();
		for (int i = 0; i < aFilePath.length; i++)
		{
			/* 判断当前输出目录是mapfile, 还是seqencefile */
			Path readFilePath = aFilePath[i];
			if(fs.isDirectory(readFilePath))
			{
				readFilePath = new Path(readFilePath, "data.avro");
			}
			
			DataFileReader<Object> reader = new DataFileReader<Object>(new FsInput(readFilePath, conf), AvroUtils.getDatumReader(conf));
			if (!mapReader.containsKey(readFilePath.toString()))
			{
				mapReader.put(readFilePath.toString(), reader);
			} 
			else
			{
				log.error("dir[" + aFilePath[i].toString() + "] has exist.");
				reader.close();
			}
		}

		return mapReader;
	}



	protected int prepareInsertModel(DBConnect dbConn, String tableName, List<String> lstFields)
	{
		/* 构建insert语句模板 */
		String feildModel = null;
		String valueModel = null;
		for (String tmpField : lstFields)
		{
			if (null != feildModel)
			{
				feildModel += ", ";
			} 
			else
			{
				feildModel = "";
			}

			if (null != valueModel)
			{
				valueModel += ", ";
			} 
			else
			{
				valueModel = "";
			}
			
			feildModel += tmpField;
			valueModel += "?";
		}

		String insertSqlModel = "insert into " + tableName + " (" + feildModel + ") values (" + valueModel + ")";
		if (true != dbConn.createInsertModel(insertSqlModel))
		{
			log.error("createInsertModel[" + insertSqlModel + "] fail.");
			return Constants.RET_INT_ERROR;
		}
		
		return Constants.RET_INT_OK;
	}
	
	public Map<String, String> getFeilds(Exportable exportKey, Exportable exportValue, String timestampName, Date timeStamp, Configuration conf)
	{
		/* 从key中获取字段 */
		Map<String, String> mapFeilds = Maps.newHashMap();
		Map<String, String> tmpKeyMap = exportKey.getExportFeilds();
		if (null == tmpKeyMap)
		{
			log.error("getFeilds: getExportFeild from key return null, exportKey is " + exportKey.getClass().getName());
			return null;
		}
		else
		{
		    /* 将key全部转成小写 */
		    for(Entry<String, String> entry : tmpKeyMap.entrySet())
		    {
		        mapFeilds.put(entry.getKey(), entry.getValue());
		    }
		}
		
		Map<String, String> tmpValueMap = exportValue.getExportFeilds();
		for (Entry<String, String> entry : tmpValueMap.entrySet())
		{
			String tmpKey = (String)entry.getKey();
			String tmpValue = entry.getValue();
			if (mapFeilds.containsKey(tmpKey))
			{
				log.error("value-feild[" + tmpKey + "] already exist");
				return null;
			}
			else
			{
		         /* 将key全部转成小写 */
				mapFeilds.put(tmpKey, tmpValue);
			}
		}

		/* 加入时间戳字段 */
		String timestampType = conf.get("timestampType");
		//String timestampType = System.getProperty("timestampType");
		if(timestampType != null && "Date".equals(timestampType))
		{
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String timestampStr = df.format(timeStamp);
			mapFeilds.put(timestampName, timestampStr);
		}else
		{
			Long timestamp = timeStamp.getTime() / 1000;
			mapFeilds.put(timestampName, timestamp.toString());
		}
		return mapFeilds;
	}
	
	public int addInsertSql(DBConnect dbConn, Map<String, String> mapFeilds, List<String> lstFields, Map<String, String> aliasNames)
	{
		if (null == mapFeilds || mapFeilds.size() <= 0)
		{
			log.error("addInsertSql: mapFeilds[" + mapFeilds.toString() + "] invalid");
			return Constants.RET_INT_ERROR;
		}

		/* 如果mapDBFeilds有效, 则将入库字段更名 */
		List<String> lstFeildValues = Lists.newArrayList();
		for(String fieldName : lstFields)
		{
			/* 根据数据表中的字段名, 找到对应的入库数据中的字段名称 */
			String originFieldName = fieldName;
			if(null != aliasNames && aliasNames.containsKey(fieldName))
			{
				originFieldName = aliasNames.get(fieldName);
			}
			
			String tmpValue = mapFeilds.get(originFieldName);
			if(null == tmpValue)
			{
			    /* 当待入库记录中没有该字段时, 字段值当0处理 */
//				log.warn("the analysis target  [" + originFieldName + "] has no value.");
			    lstFeildValues.add("0");
			}
			else
			{
			    lstFeildValues.add(tmpValue.toString());
			}
		}

		if (true != dbConn.setFeildValue(lstFeildValues))
		{
			log.error("setFeildValue[" + lstFeildValues + "] error.");
			return Constants.RET_INT_ERROR;
		}

		return Constants.RET_INT_OK;
	}
	
	private List<String> getTableFields(DBConnect dbConn, String tableName) throws Exception
	{
		List<String>  lstFields = Lists.newArrayList();
		
		ResultSet rs = dbConn.exceQuerySelectSql("desc " + tableName);
		while (rs.next())
		{
			String field = rs.getString(1);
			
			String type = rs.getString(2);
			String couldNull = rs.getString(3);
			String hasDefault = rs.getString(5);

			String extra = rs.getString(6);
			if (!("auto_increment".equalsIgnoreCase(extra) || "CURRENT_TIMESTAMP".equalsIgnoreCase(hasDefault)) )
			{
				lstFields.add(field);
			}
		}
		
		return lstFields;
	}
	
	/* 参数如下: 
	 * 1  dbConn: mysql
	 * 2  tableName
	 * 3  inputDir
	 * 4  timeValue
	 * 5  timestampName
	 * 6  fieldNames
	 * */
	public int export(DBConnect dbConn, String tableName, String inputDir, Date timeValue, String tsName, Map<String, Map<String, String>> mapAlaisNames) throws Exception
	{
		if(null == dbConn || null == tableName || null == inputDir || null == timeValue)
		{
			log.error("null == dbConn || null == tableName || null == inputDir || null == timeValue");
			return Constants.RET_INT_ERROR;
		}
		
		if(null == tsName)
		{
			tsName = Constants.FIELD_TIMESTAMP_NAME;
		}
		
		if(!dbConn.isConnOk())
		{
			if(null == dbConn.getConnection())
			{
				log.error("dbConn.getConnection() error.");
				return Constants.RET_INT_ERROR;
			}
		}

		/* 读取入库文件 */
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path inputPath = new Path(inputDir);
		if (!fs.exists(inputPath))
		{
			log.error("inputDir[" + inputPath + "] not exist when tableName is " + tableName);
//			return Constants.RET_INT_ERROR;
			return Constants.RET_INT_ERROR_NORD;
		}

		Map<String, DataFileReader<Object>> mapReaderInfo = getReaders(inputPath, fs, conf);
		if (mapReaderInfo.size() <= 0)
		{
			log.error("no files to reader when tableName is " + tableName);
			return Constants.RET_INT_ERROR;
		}

		/* 判断表中当前时间戳的数据是否存在, 存在的话删除记录, 后面重新入库 */
		String dbTsName = tsName;
		if(mapAlaisNames != null && mapAlaisNames.containsKey(tableName) )
		{			
			Map<String,String> mapTbName =  mapAlaisNames.get(tableName);
			for(Entry<String,String> entryName:mapTbName.entrySet())
			{
            	if(tsName.equalsIgnoreCase(entryName.getValue()))
            	{
        			dbTsName = entryName.getKey();
        			break;
            	}
			}			
		}
		String delSql = null;
		String timestampType = conf.get("timestampType");
		//String timestampType = System.getProperty("timestampType");
		if(timestampType !=null && "Date".equals(timestampType))
		{
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String timevalueStr = df.format(timeValue);
			delSql = "delete from " + tableName + " where " + dbTsName + "='" + timevalueStr +"'";
		}else
		{
			delSql = "delete from " + tableName + " where " + dbTsName + "='" + (timeValue.getTime() / 1000) +"'";
		}
		
		int iRet = dbConn.exceSingleResultSql(delSql);
		if (Constants.RET_INT_ERROR == iRet)
		{
			log.error("exceSingleResultSql[" + delSql + "] fail.");
			return Constants.RET_INT_ERROR;
		}
		else
		{
			log.info("exceSingleResultSql[" + delSql + "] success, and update " + iRet + " records.");
		}
		
		/* 根据数据表中的字段, 建立InsertModel */
		List<String> lstFields = getTableFields(dbConn, tableName);
		if(null == lstFields || lstFields.size() <= 0)
		{
			log.error("null == lstFields || lstFields.size() <= 0 when tableName=" + tableName);
			return Constants.RET_INT_ERROR;
		}
		
		if(Constants.RET_INT_OK != prepareInsertModel(dbConn, tableName, lstFields))
		{
			log.error("prepareInsertModel error. when tableName=" + tableName + ", mapFeilds=" + lstFields.toString());
			return Constants.RET_INT_ERROR;
		}
		
		Long lTotalReadNum = 0L;
		Long lTotalExcNum = 0L;
		Long lTotalInsertNum = 0L;
		Long lTotalInsertSuccNum = 0L;
		TableCheck fieldCheck = new TableCheck(dbConn,tableName);
		for (Entry<String, DataFileReader<Object>> readerInfo : mapReaderInfo.entrySet())
		{
			String readFile = readerInfo.getKey();
			DataFileReader<Object> reader = readerInfo.getValue();

			Long lCommitNum = 0L;
			Long lReadNum = 0L;
			Long lInsertNum = 0L;
			Long lInsertSuccNum = 0L;
			Long lExcNum = 0L;
			while (reader.hasNext())
			{
				/* 对每个文件中的记录计数, 打印输出 */
			    lReadNum++;
				Object value = reader.next();
				if(!(value instanceof Pair))
				{
					lExcNum++;
					log.error("value not instanceof Pair");
					continue;
				}
				Pair<Object, Object> pair = (Pair)value;
				
				Object exportKey = pair.key();
				Object exportValue = pair.value();
				if(!(exportKey instanceof Exportable && exportValue instanceof Exportable))
				{
				    dbConn.rollback();
				    dbConn.clearModel();

				    lExcNum++;
					log.error("!(exportKey instanceof Exportable && exportValue instanceof Exportable), exportKey's classname=" +
				               exportKey.getClass().getName() + " exportValue classname=" + exportValue.getClass().getName());
					return Constants.RET_INT_ERROR;
				}
				
				Map<String, String> mapFeilds = getFeilds((Exportable)exportKey, (Exportable)exportValue, tsName, timeValue, conf);
				if(null == mapFeilds)
				{
                    dbConn.rollback();
                    dbConn.clearModel();
                    
				    lExcNum++;
					log.error("null == mapFeilds when exportKey=" + exportKey.toString() + ", exportValue=" + exportValue.toString());
					return Constants.RET_INT_ERROR;
				}
				//将异常数据入异常表中
				if (!fieldCheck.checkField(mapFeilds))
				{
					continue;
				}
				/* 根据报表配置, 获取入库sql语句 */
				
				if (Constants.RET_INT_OK != addInsertSql(dbConn, mapFeilds, lstFields, 
						mapAlaisNames == null ? null : mapAlaisNames.get(tableName)))
				{
					lExcNum++;
					continue;
				}

				lInsertNum++;
				
				lCommitNum++;
				if (lCommitNum >= DB_COMMIT_TIMES)
				{
				    lCommitNum = 0L;
				    int dbExc = dbConn.exceModel();
				    lInsertSuccNum += ( (dbExc < 0 ) ? 0 : dbExc );
				}
				if( lReadNum % 10000 == 0 )
				{
					log.info("read " + lReadNum + ", Exc " + lExcNum + ", insert" + lInsertNum + ", insertSucc " + lInsertSuccNum + " from " + readFile);
				}
			}

			//发现批量入库 有时会出现当文件中有一个入库异常时，整个文件入库失败，暂时改为单条入库
//			lInsertSuccNum += dbConn.exceModel();

			lTotalReadNum += lReadNum;
			lTotalExcNum += lExcNum;
			lTotalInsertNum += lInsertNum;
			lTotalInsertSuccNum += lInsertSuccNum;
			log.info("total read " + lReadNum + ", Exc " + lExcNum + ", insert" + lInsertNum + ", insertSucc " + lInsertSuccNum + " from " + readFile);
		}

		if (Constants.RET_INT_OK == dbConn.commit()) {
			dbConn.closeModel();
			log.info("table " + tableName + ": total read " + lTotalReadNum + ", Exc " + lTotalExcNum + ", insert" + lTotalInsertNum + ", insertSucc " + lTotalInsertSuccNum + " records");
			return Constants.RET_INT_OK;
		} else {
			dbConn.closeModel();
			log.error("table " + tableName + ": total read " + lTotalReadNum + ", Exc " + lTotalExcNum + ", insert" + lTotalInsertNum + ", insertSucc " + 0 + " records");
			return Constants.RET_INT_ERROR;
		}

	}
}
