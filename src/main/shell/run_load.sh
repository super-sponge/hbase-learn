!#/bin/bash


for f in $HBASE_HOME/lib/*.jar; do
  if [ "$HADOOP_CLASSPATH" ]; then
    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$f
  else
    export HADOOP_CLASSPATH=$f
  fi
done



hdfs dfs -rmr hdfs://xj2:9000/tmp/hbasetest/output

hadoop  jar /home/hadoop/app/hbase-1.2.2/lib/hbase-server-1.2.2.jar importtsv -Dimporttsv.columns=HBASE_ROW_KEY,info:name,info:age,info:phone -Dimporttsv.bulk.output=/tmp/hbasetest/output student /tmp/hbasetest/input

hadoop jar /home/hadoop/app/hbase-1.2.2/lib/hbase-server-1.2.2.jar completebulkload /tmp/hbasetest/output student 

#自定义bulkload函数
#hadoop jar hbase-learn-1.0-SNAPSHOT.jar hbase.learn.HBaseBulkLoad student  /tmp/hbasetest/input /tmp/hbasetest/output1

#hadoop jar /home/hadoop/app/hbase-1.2.2/lib/hbase-server-1.2.2.jar completebulkload /tmp/hbasetest/output1 student