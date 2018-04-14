# Guitar
Guitar来源于科大讯飞消费者BG平台研发部语音云数据分析团队，是讯飞首个公司级开源软件项目。<br>
Guitar是一款简单、高效的分布式多维BI报表分析引擎。主要用于在Hadoop环境下，基于MapReduce计算引擎，结合业界流行的大数据统计分析技术和OLAP cube思想，以Groovy脚本及XML配置方式，简单、灵活、高效地出具从原始日志到多维报表分析结果。

## Guitar的特点
（1）入门上手简单：使用者仅需编写一个Groovy脚本、配置一个XML文件，完全屏蔽诸如MapReduce、Spark等大数据编程开发；<br>
（2）分析策略灵活：数据的分析逻辑完全由使用者的Groovy脚本自定义，且提供自实现、可配置、自动化3种方式的cube构建方式；<br>
（3）程序运行高效：利用MOLAP cube思想，结合Bitmap、HyperLogLog等uv算法，配以自研发的高性能avro数据中间件，高效计算各项维度组合下的各项指标；<br>
（4）适应场景丰富：支持多种格式数据源和多种数据库表展现，提供8种计算指标，8种时间粒度，且接口简单、极易扩展，满足多数常见的大数据BI报表分析需要。

 ![](https://github.com/iflytek/Guitar/blob/master/logo.jpg) 

## TestCases
项目中test_data文件夹提供了一份改自于搜狗实验室公开的测试数据集[用户查询日志](http://www.sogou.com/labs/resource/q.php)，分别是文本格式和parquet格式。<br>
项目中guitar文件夹提供了两份应用测试数据集出具的样例报表，其中guitar\report_def\test\AreaBrowserInfo是浏览器地域分布信息小时报表，guitar\report_def\test\WordsQueryInfo是短语搜索信息天报表。

## Quick Start
（1）下载Guitar项目至本地目录，导入IDE中（推荐使用IntelliJ IDEA，若使用Eclipse可能需要将项目中的test_data和guitar文件夹移动到guitar-core目录下）。<br>
（2）在可连接的mysql服务器的test库中新建两张表，建表语句如下：

    CREATE TABLE `AreaBrowserInfoHourly`  (
      `area` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT 'EMPTY' COMMENT '地域',
      `browser` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT 'EMPTY' COMMENT '浏览器品牌',
      `sess_times` bigint(20) NULL DEFAULT 0 COMMENT '使用次数',
      `users` bigint(20) NULL DEFAULT 0 COMMENT '使用人数',
      `timestamp` bigint(20) NOT NULL DEFAULT 0 COMMENT '时间戳',
      PRIMARY KEY (`area`, `browser`, `timestamp`) USING BTREE,
      UNIQUE INDEX `basic_info_key`(`timestamp`, `area`, `browser`) USING BTREE
    ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_bin ROW_FORMAT = Dynamic;

    CREATE TABLE `WordsQueryInfoDaily`  (
      `words` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT 'EMPTY' COMMENT '搜索词语',
      `sess_times` bigint(20) NULL DEFAULT 0 COMMENT '使用次数',
      `users` bigint(20) NULL DEFAULT 0 COMMENT '使用人数',
      `maxClickDeep` bigint(20) NULL DEFAULT 0 COMMENT '最大点击深度',
      `timestamp` bigint(20) NOT NULL DEFAULT 0 COMMENT '时间戳',
      PRIMARY KEY (`words`, `timestamp`) USING BTREE,
      UNIQUE INDEX `basic_info_key`(`timestamp`, `words`) USING BTREE
    ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_bin ROW_FORMAT = Dynamic;
（3）将两表的连接信息写入guitar\report_def\test\AreaBrowserInfo\report.def.xml和guitar\report_def\test\WordsQueryInfo\report.def.xml中对应位置。<br>
注：样例默认mysql服务器ip="localhost"，端口port="3306"，用户名user="root"，密码passwd="123456"，数据库dbName="test"。<br>
（4）Guitar支持本地local模式直接运行和Hadoop集群jar包运行。<br>
无须额外配置，直接打开com.iflytek.guitar.core.main.ReportMain类，<br>
给予参数test.AreaBrowserInfo 2018-01-12T04:00Z Hourly true即可运行浏览器地域分布信息2018-01-12 04点小时报表，<br>
给予参数test.WordsQueryInfo 2018-01-12T00:00Z Daily true即可运行短语搜索信息2018-01-12天报表。<br>
（5）运行完毕，报表结果数据入库对应表中。其中维度字段为ALL的代表不区分该字段时各指标的分布情况，这样可以将mysql报表数据的分组查询转为单点查询。<br>
以浏览器地域分布信息小时报表为例：该报表功能是读取guitar\test_data\txt\2018-01-12\04\SogouQ_sample_rev.txt样例日志，分析各个浏览器在各个地域下的使用次数和用户数。<br>
报表结果中地域area字段为ALL的情况，代表各个浏览器在所有地域情况下的使用次数和用户数，而浏览器browser字段为ALL的情况，则代表各地域在不分浏览器情况下的使用次数和用户数。<br>
换个SQL说法，即：

    SELECT
    	browser,
    	sess_times
    FROM
    	AreaBrowserInfoHourly 
    WHERE
    	`timestamp` = UNIX_TIMESTAMP( '2018-01-12 04:00:00' ) 
    	AND area = 'ALL';
  等于
  
     SELECT
    	browser,
    	SUM(sess_times)
    FROM
    	AreaBrowserInfoHourly 
    WHERE
    	`timestamp` = UNIX_TIMESTAMP( '2018-01-12 04:00:00' ) 
    	AND area <> 'ALL'
    GROUP BY browser;
       
（6）在Hadoop集群上运行<br>
  将项目中的test_data和guitar文件夹上传到运行账户的目录下，如：hadoop fs -put test_data /user/xxx；<br>
  进入项目主目录，执行mvn clean install -Dmaven.test.skip=true；<br>
  进入项目guitar-core目录，执行mvn clean assembly:assembly -Dmaven.test.skip=true，得到guitar-core-0.4-SNAPSHOT-jar-with-dependencies.jar；<br>
  在Hadoop客户端执行hadoop jar guitar-core-0.4-SNAPSHOT-jar-with-dependencies.jar com.iflytek.guitar.core.main.ReportMain test.AreaBrowserInfo 2018-01-12T04:00Z Hourly true -Dmapreduce.job.queuename=default即可运行浏览器地域分布信息小时报表。<br>
  同理运行短语搜索信息天报表。

## License
Guitar是在Apache 2.0开源协议许可下发布的。

    Copyright 1999-2018 IFLYTEK CO.,LTD.
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
         http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
