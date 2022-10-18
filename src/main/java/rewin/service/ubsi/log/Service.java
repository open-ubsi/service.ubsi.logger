package rewin.service.ubsi.log;

import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.client.*;
import org.bson.Document;
import rewin.ubsi.annotation.*;
import rewin.ubsi.common.JedisUtil;
import rewin.ubsi.common.LogUtil;
import rewin.ubsi.common.MongoUtil;
import rewin.ubsi.common.Util;
import rewin.ubsi.consumer.Context;
import rewin.ubsi.container.Bootstrap;
import rewin.ubsi.container.ServiceContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * UBSI日志服务：rewin.ubsi.logger -> rewin.service.ubsi.log.Service
 */
@UService(
        name = "rewin.ubsi.logger",
        tips = "UBSI日志服务（有内部缓冲，部署多实例时需要Redis环境）",
        version = "1.0.0",
        container = "2.3.0",
        release = false         // false表示社区版
)
public class Service extends ServiceEntry {

    static MongoUtil.Config MongoDBConfig = null;       // 当前MongoDB配置
    static MongoClient      MongoDBClient = null;       // 当前MongoDB客户端实例
    static MongoDatabase    MongoDBLogs = null;         // ubsi_logs数据库

    // 日志表的缺省索引
    static List<List<Database.Index>> IndexLogs = Arrays.asList(
            Arrays.asList(new Database.Index(Database.LOGS_TIME, 1)),
            Arrays.asList(new Database.Index(Database.LOGS_TYPE, 1)),
            Arrays.asList(new Database.Index(Database.LOGS_APP, 1))
        );

    // 缺省的Filter - logs_ubsi_access
    static Database.Filter  FilterUbsiAccess = new Database.Filter();
    static {
        FilterUbsiAccess._id = "ubsi_access";
        FilterUbsiAccess.type = Arrays.asList(LogUtil.ACCESS);
        FilterUbsiAccess.app = Arrays.asList(Bootstrap.LOG_APPID, Context.LOG_APPID);
        FilterUbsiAccess.index = Arrays.asList(
                Arrays.asList(new Database.Index(Database.LOGS_TIME, 1)),
                Arrays.asList(new Database.Index(Database.LOGS_APP, 1)),
                Arrays.asList(new Database.Index("body.reqId", 1)),
                Arrays.asList(new Database.Index("body.seqId", 1)),
                Arrays.asList(new Database.Index("body.service", 1), new Database.Index("body.entry", 1))
        );
    }

    // 加载的Filters
    static ConcurrentMap<String, Database.Filter> Filters = new ConcurrentHashMap<>();
    final static String FILTERS_CHANNEL = "ubsi_logs_filters";  // 订阅频道
    static JedisUtil.Listener Listener = null;      // Filters变化的订阅器
    static String ListenerID = Util.getUUID();      // 订阅器的ID
    static BatchDeal BatchDealer = null;            // 批量处理器

    // 创建索引
    static void createIndex(String cname, List<List<Database.Index>> indexes) {
        if ( indexes == null )
            return;
        MongoCollection col = MongoDBLogs.getCollection(cname);
        for ( List<Database.Index> index : indexes ) {
            Document doc = new Document();
            for ( Database.Index idx : index )
                doc.append(idx.key, idx.type == 0 ? "text" : idx.type);
            col.createIndex(doc);
        }
    }

    // 新增Filter
    static void createFilter(Database.Filter filter) {
        MongoCollection<Database.Filter> col = MongoDBLogs.getCollection(Database.COL_FILTERS, Database.Filter.class);
        col.insertOne(filter);

        String cname = Database.COL_LOGS + "_" + filter._id;
        MongoDBLogs.createCollection(cname);        // 新建日志数据表

        BatchDealer.addFilter(filter._id);

        Filters.put(filter._id, filter);
        if ( FilterUbsiAccess != filter && JedisUtil.isInited() )
            JedisUtil.publish(FILTERS_CHANNEL, ListenerID);     // 发出消息通知

        createIndex(cname, filter.index);           // 新建日志数据表索引
    }

    // 加载Filters
    static void loadFilters() {
        BatchDealer.wakeup();
        MongoCollection<Database.Filter> col = MongoDBLogs.getCollection(Database.COL_FILTERS, Database.Filter.class)
                .withReadPreference(ReadPreference.primary());
        List<Database.Filter> list = col.find().into(new ArrayList<>());

        Filters.clear();
        BatchDealer.clearFilter();
        for ( Database.Filter filter : col.find() ) {
            Filters.put(filter._id, filter);
            BatchDealer.addFilter(filter._id);
        }
    }

    /** 初始化 */
    @USInit
    public static void init(ServiceContext ctx) throws Exception {
        MongoDBConfig = ctx.readDataFile(MongoUtil.CONFIG_FILE, MongoUtil.Config.class);
        MongoDBClient = MongoUtil.getMongoClient(MongoDBConfig, Database.DB);
        MongoDBLogs = MongoDBClient.getDatabase(Database.DB);

        BatchDealer = new BatchDeal();
        BatchDealer.start();

        // 检查缺省的Collection是否存在
        boolean hasLogs = false;
        boolean hasFilters = false;
        for ( String cname : MongoDBLogs.listCollectionNames() ) {
            if ( Database.COL_LOGS.equals(cname) )
                hasLogs = true;
            else if ( Database.COL_FILTERS.equals(cname) )
                hasFilters = true;
        }
        if ( !hasLogs ) {
            // 创建主表
            MongoDBLogs.createCollection(Database.COL_LOGS);
            createIndex(Database.COL_LOGS, IndexLogs);
        }
        if ( !hasFilters ) {
            // 创建缺省的Filter表（logs_ubsi_access）
            MongoDBLogs.createCollection(Database.COL_FILTERS);
            createFilter(FilterUbsiAccess);
        } else
            loadFilters();  // 加载Filters

        if ( JedisUtil.isInited() ) {
            // 订阅Filters的变更
            Listener = new JedisUtil.Listener() {
                @Override
                public void onMessage(String channel, Object msg) throws Exception {
                    if ( !FILTERS_CHANNEL.equals(channel) || ListenerID.equals(msg) )
                        return;
                    try {
                        loadFilters();
                    } catch (Exception e) {
                        ctx.getLogger().error("reload filters", e);
                    }
                }
                @Override
                public void onEvent(String channel, Object event) throws Exception {
                }
            };
            Listener.subscribe(FILTERS_CHANNEL);
        }
    }

    /** 结束 */
    @USClose
    public static void close(ServiceContext ctx) throws Exception {
        Filters.clear();
        if ( Listener != null ) {
            if ( JedisUtil.isInited() )
                Listener.unsubscribe();
            Listener = null;
        }

        if ( BatchDealer != null ) {
            BatchDealer.close();
            BatchDealer = null;
        }

        if ( MongoDBClient != null ) {
            MongoDBClient.close();
            MongoDBClient = null;
        }
        MongoDBConfig = null;
        MongoDBLogs = null;
    }

    /** 运行信息 */
    @USInfo
    public static Map info(ServiceContext ctx) throws Exception {
        if ( BatchDealer == null )
            return null;
        return Util.toMap(new Object[] { "recorded_new_logs", BatchDealer.Count.get() });
    }

    /** 返回配置参数 */
    @USConfigGet
    public static Config getConfig(ServiceContext ctx) throws Exception {
        Config res = new Config();
        if ( MongoDBConfig != null ) {
            res.mongo_servers = MongoDBConfig.servers;
            res.mongo_auth = MongoDBConfig.auth;
            if ( res.mongo_auth != null )
                for ( MongoUtil.Auth auth : res.mongo_auth )
                    if ( auth.username != null )
                        auth.password = "******";
            res.mongo_option = MongoDBConfig.option;
        }
        MongoUtil.Config config = ctx.readDataFile(MongoUtil.CONFIG_FILE, MongoUtil.Config.class);
        if ( config != null ) {
            res.mongo_servers_restart = config.servers;
            res.mongo_auth_restart = config.auth;
            if ( res.mongo_auth_restart != null )
                for ( MongoUtil.Auth auth : res.mongo_auth_restart )
                    if ( auth.username != null )
                        auth.password = "******";
            res.mongo_option_restart = config.option;
        }
        return res;
    }

    /** 设置配置参数 */
    @USConfigSet
    public static void setConfig(ServiceContext ctx, String json) throws Exception {
        Config cfg = Util.json2Type(json, Config.class);
        MongoUtil.Config config = new MongoUtil.Config();
        config.servers = cfg.mongo_servers;
        config.auth = cfg.mongo_auth;
        config.option = cfg.mongo_option;

        MongoUtil.checkConfig(config);
        ctx.saveDataFile(MongoUtil.CONFIG_FILE, config);
    }

}
