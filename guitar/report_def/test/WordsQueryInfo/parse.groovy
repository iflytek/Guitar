/*
CREATE TABLE `test`.`WordsQueryInfoDaily`  (
  `words` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT 'EMPTY' COMMENT '搜索词语',
  `sess_times` bigint(20) NULL DEFAULT 0 COMMENT '使用次数',
  `users` bigint(20) NULL DEFAULT 0 COMMENT '使用人数',
  `maxClickDeep` bigint(20) NULL DEFAULT 0 COMMENT '最大点击深度',
  `timestamp` bigint(20) NOT NULL DEFAULT 0 COMMENT '时间戳',
  PRIMARY KEY (`words`, `timestamp`),
  UNIQUE INDEX `basic_info_key`(`timestamp`, `words`) USING BTREE
);
*/

import com.iflytek.guitar.core.util.ScriptOper
import com.iflytek.guitar.core.util.ScriptMap
import com.iflytek.guitar.core.data.dataformat.DataRecord

public class TestScript extends ScriptOper {
    public void parse(DataRecord record, List<ScriptMap> lstMap) {

        String uid = record.getString("uid", CUSTOBJECT.VALUE_EMPTY)
        String queryWords = record.getString("queryWords", CUSTOBJECT.VALUE_EMPTY)
        String sequenceNumber = record.getString("sequenceNumber", CUSTOBJECT.VALUE_EMPTY)

        ScriptMap map = ScriptMap.getScriptMap()

        map.put("words", queryWords)

        map.put("sess_times", 1L)

        if (!CUSTOBJECT.isEmpty(uid)) {
            map.put("users", uid)
        }

        map.put("maxClickDeep", Integer.valueOf(sequenceNumber))

        lstMap.add(map)

    }

}
