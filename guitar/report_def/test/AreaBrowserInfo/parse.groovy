/*
CREATE TABLE `test`.`AreaBrowserInfoHourly`  (
  `area` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT 'EMPTY' COMMENT '地域',
  `browser` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT 'EMPTY' COMMENT '浏览器品牌',
  `sess_times` bigint(20) NULL DEFAULT 0 COMMENT '使用次数',
  `users` bigint(20) NULL DEFAULT 0 COMMENT '使用人数',
  `timestamp` bigint(20) NOT NULL DEFAULT 0 COMMENT '时间戳',
  PRIMARY KEY (`area`, `browser`, `timestamp`),
  UNIQUE INDEX `basic_info_key`(`timestamp`, `area`, `browser`) USING BTREE
);
*/

import com.iflytek.guitar.core.util.ScriptOper
import com.iflytek.guitar.core.util.ScriptMap
import com.iflytek.guitar.core.data.dataformat.DataRecord

public class TestScript extends ScriptOper {
    public void parse(DataRecord record, List<ScriptMap> lstMap) {

        String line = record.getData()

        String[] fields = line.split("\t", -1)

        String province = fields[0]
        String browser = fields[1]
        String uid = fields[2]
        String queryWords = fields[3]
        String rank = fields[4]
        String sequenceNumber = fields[5]
        String clickURL = fields[6]

        ScriptMap map = ScriptMap.getScriptMap()

        map.put("province", province)
        map.put("browser",browser)

        map.put("sess_times", 1L)

        if (!CUSTOBJECT.isEmpty(uid)) {
            map.put("users", uid)
        }

        lstMap.add(map)

    }

}
