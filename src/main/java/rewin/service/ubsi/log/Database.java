package rewin.service.ubsi.log;

import java.util.List;

/**
 * MongoDB数据库表结构
 */
public class Database {

    public final static String DB = "ubsi_logs";            // Database的名字
    public final static String COL_LOGS = "logs";           // Collection的名字（body字段为可变类型，所以不能使用PojoCodec）
    public final static String COL_FILTERS = "filters";     // Collection的名字

    /** COL_LOGS表的字段名字 */
    public static String LOGS_TIME = "time";                // long, 时间戳
    public static String LOGS_TYPE = "type";                // int, 日志分类
    public static String LOGS_ADDR = "addr";                // String, 应用位置
    public static String LOGS_TAG = "tag";                  // String, 应用分组
    public static String LOGS_APP = "app";                  // String, 应用ID
    public static String LOGS_CODE = "code";                // String, 代码位置(调用类#方法#行号)
    public static String LOGS_TIPS = "tips";                // String, 日志说明
    public static String LOGS_BODY = "body";                // Object, 日志内容

    /* 索引 */
    public static class Index {
        public String   key;        // 字段名字
        public int      type;       // 索引类型，0:全文，1:升序，-1:降序

        public Index() {}           // 必须有"空"的构造函数，以便MongoDB的PojoCodec使用
        public Index(String key, int type) {
            this.key = key;
            this.type = type;
        }
    }

    /** COL_FILTERS表的结构 */
    public static class Filter {
        public String               _id;            // 名字，不能重复，作为表名字的后缀
        public List<Integer>        type;           // 日志分类
        public List<String>         tag;            // 应用分类
        public List<String>         app;            // 应用ID
        public List<List<Index>>    index;          // 索引
    }
}
