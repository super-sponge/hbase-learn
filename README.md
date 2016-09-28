#hbase learn

##bulkload
###实例文件
    1001    lilei   17  13800001111
    1002    lily    16  13800001112
    1003    lucy    16  13800001113
    1004    meimei  16  13800001114
    
###hbase 对应的表
    create 'student', {NAME => 'info'}
    
###执行importtsv 导入数据
        hadoop  jar /home/hadoop/app/hbase-1.2.2/lib/hbase-server-1.2.2.jar importtsv -Dimporttsv.columns=HBASE_ROW_KEY,info:name,info:age,info:phone -Dimporttsv.bulk.output=/tmp/hbasetest/output student /tmp/hbasetest/input  
    
    没有指定-Dimporttsv.bulk.output, importtsv默认行为是才有client api的put来导入数据于hbase, 指定-Dimporttsv.bulk.output, 则需要下一步
        hadoop jar /home/hadoop/app/hbase-1.2.2/lib/hbase-server-1.2.2.jar completebulkload /tmp/hbasetest/output student 
        
###数据验证
    scan 'student', {LIMIT => 10}
    
###编写代码
    借助maven的assembly插件, 生成胖jar包(就是把依赖的zookeeper和hbase jar包都打到该MapReduce包中), 否则的话, 就需要用户静态配置, 在Hadoop的class中添加zookeeper和hbase的配置文件和相关jar包.
     
    
参考：
    http://blog.csdn.net/lifuxiangcaohui/article/details/41831975