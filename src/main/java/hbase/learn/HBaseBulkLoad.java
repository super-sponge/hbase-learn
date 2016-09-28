package hbase.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by liuhb on 2016/9/27.
 */
public class HBaseBulkLoad {
    public static class BulkMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Cell> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] terms = line.split("\t");
            if ( terms.length == 4 ) {
                byte[] rowkey = terms[0].getBytes();
                ImmutableBytesWritable imrowkey = new ImmutableBytesWritable(rowkey);
                // 写入context中, rowkey => keyvalue, 列族:列名  info:name, info:age, info:phone

                context.write(imrowkey, createCell(rowkey, Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(terms[1])));
                context.write(imrowkey, createCell(rowkey, Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(terms[2])));
                context.write(imrowkey, createCell(rowkey, Bytes.toBytes("info"), Bytes.toBytes("phone"), Bytes.toBytes(terms[3])));
            }
        }

        private static Cell createCell(byte[] rowkey, byte[] family, byte[] qualifier, byte[] value) {
            return CellUtil.createCell(rowkey, family, qualifier, HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put.getCode(), value);
        }
    }
    public static void main(String[] args) throws Exception {
        if ( args.length != 3 ) {
            System.err.println("Usage: MyBulkload <table_name> <data_input_path> <hfile_output_path>");
            System.exit(2);
        }
        String tableName = args[0];
        String inputPath = args[1];
        String outputPath= args[2];

        // 创建的HTable实例用于, 用于获取导入表的元信息, 包括region的key范围划分
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf(tableName));
        RegionLocator regionLocator = conn.getRegionLocator(TableName.valueOf(tableName));

        Job job = Job.getInstance(conf, "Bulkload");

        job.setMapperClass(BulkMapper.class);

        job.setJarByClass(HBaseBulkLoad.class);
        job.setInputFormatClass(TextInputFormat.class);

        // 最重要的配置代码, 需要重点分析
        HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor(), regionLocator);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
