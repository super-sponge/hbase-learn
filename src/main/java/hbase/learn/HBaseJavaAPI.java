package hbase.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuhb on 2016/9/22.
 *
 *
 Scan类常用方法说明
 指定需要的family或column ，如果没有调用任何addFamily或Column，会返回所有的columns；
  scan.addFamily();
  scan.addColumn();
  scan.setMaxVersions(); //指定最大的版本个数。如果不带任何参数调用setMaxVersions，表示取所有的版本。如果不掉用setMaxVersions，只会取到最新的版本.
  scan.setTimeRange(); //指定最大的时间戳和最小的时间戳，只有在此范围内的cell才能被获取.
  scan.setTimeStamp(); //指定时间戳
  scan.setFilter(); //指定Filter来过滤掉不需要的信息
  scan.setStartRow(); //指定开始的行。如果不调用，则从表头开始；
  scan.setStopRow(); //指定结束的行（不含此行）；
  scan.setBatch(); //指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。

 过滤器
 1、FilterList代表一个过滤器列表
 FilterList.Operator.MUST_PASS_ALL -->and
 FilterList.Operator.MUST_PASS_ONE -->or
 eg、FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
 2、SingleColumnValueFilter
 3、ColumnPrefixFilter用于指定列名前缀值相等
 4、MultipleColumnPrefixFilter和ColumnPrefixFilter行为差不多，但可以指定多个前缀。
 5、QualifierFilter是基于列名的过滤器。
 6、RowFilter
 7、RegexStringComparator是支持正则表达式的比较器。
 8、SubstringComparator用于检测一个子串是否存在于值中，大小写不敏感。

 */
public class HBaseJavaAPI {
    private static Configuration conf = null;
    private static Connection conn = null;

    private static  final String tableName = "student";


    static {
        conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "localhost");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");

    }

    private static Connection getConn() throws IOException {
        if (conn == null) {
            conn = ConnectionFactory.createConnection(conf);
        }
        return conn;
    }

    private static boolean isExist(String tableName) throws IOException {
        Admin admin = getConn().getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public static void createTable(String tableName, String[] colFamilys) throws IOException {

        if (isExist(tableName)) {
            System.out.println("表 " + tableName + " 已存在！");
            return;
        }
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
        for (String colFamily : colFamilys) {
            tableDesc.addFamily(new HColumnDescriptor(colFamily));
        }
        Admin admin = getConn().getAdmin();
        admin.createTable(tableDesc);
        admin.close();
    }

    public static void deleteTable(String tableName) throws IOException {
        Admin admin = getConn().getAdmin();
        TableName tbName = TableName.valueOf(tableName);
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("删除的表 " + tableName + " 不存在！");
        } else {
            admin.disableTable(tbName);
            admin.deleteTable(tbName);
            System.out.println("删除表 " + tableName + " 成功！");
        }
        admin.close();
    }

    public static void addRow(Table table, String rowKey, String colFamily, String column, String value) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }

    public static void addRow(String table, String rowKey, String colFamily, String column, String value) throws IOException {
        Table htable = getConn().getTable(TableName.valueOf(table));
        addRow(htable, rowKey, colFamily, column, value);
        htable.close();
    }

    public static void delRow(String table, String rowKey) throws IOException {
        Table htable = getConn().getTable(TableName.valueOf(table));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        htable.delete(delete);
        htable.close();
    }

    public static void delMultiRows(String table, String[] rowKeys) throws IOException {
        Table htable = getConn().getTable(TableName.valueOf(table));
        List<Delete> delList = new ArrayList<Delete>();
        for (String rowkey : rowKeys) {
            delList.add(new Delete(Bytes.toBytes(rowkey)));
        }
        htable.delete(delList);
        htable.close();
    }

    public static void getRow(String table, String rowKey) throws IOException {
        Table htable = getConn().getTable(TableName.valueOf(table));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = htable.get(get);
        for (Cell cell : result.rawCells()) {
            printCell(cell);
        }
    }

    public static void getAllRows(String table) throws IOException {
        Table htable = getConn().getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        ResultScanner results = htable.getScanner(scan);
        // 输出结果
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                printCell(cell);
            }
        }
    }

    public static void getAllRows(String table, String[] colFamilys) throws IOException {
        Table htable = getConn().getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        for (String colFamily : colFamilys) {
            scan.addFamily(Bytes.toBytes(colFamily));
        }
        ResultScanner results = htable.getScanner(scan);
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                printCell(cell);
            }
        }
    }

    public static void getAllRows(String table, String[] colFamilys, Filter filter) throws IOException {
        Table htable = getConn().getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        for (String colFamily : colFamilys) {
            scan.addFamily(Bytes.toBytes(colFamily));
        }
        if (filter != null) {
            scan.setFilter(filter);
        }
        ResultScanner results = htable.getScanner(scan);
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                printCell(cell);
            }
        }
    }

    private static void printCell(Cell cell) {
        System.out.print("行名:" + Bytes.toString(CellUtil.cloneRow(cell)) + " ");
        System.out.print("时间戳:" + cell.getTimestamp() + " ");
        System.out.print("列族名:" + Bytes.toString(CellUtil.cloneFamily(cell)) + " ");
        System.out.print("列名:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + " ");
        System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
    }

    private static void addStudentRow(
            String rowKey, String ch, String math, String en, String sex, String age) throws IOException {

        HBaseJavaAPI.addRow(tableName, rowKey, "info", "age", age);
        HBaseJavaAPI.addRow(tableName, rowKey, "info", "sex", sex);
        HBaseJavaAPI.addRow(tableName, rowKey, "course", "china", ch);
        HBaseJavaAPI.addRow(tableName, rowKey, "course", "math", math);
        HBaseJavaAPI.addRow(tableName, rowKey, "course", "english", en);
    }

    public static void main(String[] args) throws IOException {

        // 第一步：创建数据库表：“student”
        String[] columnFamilys = {"info", "course"};
        HBaseJavaAPI.createTable(tableName, columnFamilys);
        // 第二步：向数据表的添加数据
        if (isExist(tableName)) {
            addStudentRow("zpc","97","128","85","boy","20");
            addStudentRow("henjun","90","120","90","boy","19");
            addStudentRow("wangjun","100","100","99","girl","18");
            addStudentRow("lijie","75","99","78","girl","19");
            addStudentRow("lishuang","80","98","78","boy","19");
            addStudentRow("zhaoyun","75","99","78","girl","2");

            // 第三步：获取一条数据
            System.out.println("**************获取一条(zpc)数据*************");
            HBaseJavaAPI.getRow(tableName, "zpc");
            // 第四步：获取所有数据
            System.out.println("**************获取所有数据***************");
            HBaseJavaAPI.getAllRows(tableName);
            System.out.println("**************获取所有数据 info 列簇***************");
            HBaseJavaAPI.getAllRows(tableName, new String[]{"info"});
            System.out.println("**************获取filter sex 为girl数据***************");
            Filter filter = new SingleColumnValueFilter(
                    Bytes.toBytes("info"), Bytes.toBytes("sex"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("girl"));
            HBaseJavaAPI.getAllRows(tableName, new String[]{"info","course"}, filter);

            System.out.println("**************获取filter sex 为girl 且 age > 18数据***************");
            FilterList filterList=new FilterList();
            filterList.addFilter(filter);
            Filter filterScore = new SingleColumnValueFilter(
                    Bytes.toBytes("info"), Bytes.toBytes("age"), CompareFilter.CompareOp.GREATER, Bytes.toBytes("18"));
            filterList.addFilter(filterScore);
            HBaseJavaAPI.getAllRows(tableName, new String[]{"info","course"}, filterList);
            System.out.println("**************获取filter rowkey 以l开始数据***************");
            //new SubstringComparator("li") 包涵li的查询
            Filter rowfilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new BinaryPrefixComparator(Bytes.toBytes("li")));
            HBaseJavaAPI.getAllRows(tableName, new String[]{"info","course"}, rowfilter);



            // 第五步：删除一条数据
            System.out.println("************删除一条(zpc)数据************");
            HBaseJavaAPI.delRow(tableName, "zpc");
            HBaseJavaAPI.getAllRows(tableName);
            // 第六步：删除多条数据
            System.out.println("**************删除多条数据***************");
            String rows[] = new String[]{"qingqing", "xiaoxue"};
            HBaseJavaAPI.delMultiRows(tableName, rows);
            HBaseJavaAPI.getAllRows(tableName);
            // 第七步：删除数据库
            System.out.println("***************删除数据库表**************");
            HBaseJavaAPI.deleteTable(tableName);
            System.out.println("表" + tableName + "存在吗？" + isExist(tableName));
        } else {
            System.out.println(tableName + "此数据库表不存在！");
        }
    }
}
