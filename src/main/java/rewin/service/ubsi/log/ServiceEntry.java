package rewin.service.ubsi.log;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import rewin.ubsi.annotation.USEntry;
import rewin.ubsi.annotation.USParam;
import rewin.ubsi.common.Codec;
import rewin.ubsi.common.JedisUtil;
import rewin.ubsi.common.MongoUtil;
import rewin.ubsi.common.Util;
import rewin.ubsi.container.ServiceContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * UBSI日志服务的接口
 */
public class ServiceEntry {

    @USEntry(
            tips = "记录日志",
            params = { @USParam(
                    name = "data",
                    tips = "日志数据，结构：[" +
                            "long time: 时间戳(毫秒), " +
                            "int type: 分类, " +
                            "String addr: 应用位置, " +
                            "String tag: 应用分类, " +
                            "String app: 应用ID, " +
                            "String code: 调用类#方法#行号, " +
                            "String tips: 日志说明, " +
                            "Object body: 日志内容]"
            )},
            readonly = true         // 写日志不会改变服务运行模式，所以此接口设置为readonly
    )
    public void log(ServiceContext ctx, Object[] data) {
        try {
            Integer type = (Integer) data[1];
            String tag = (String) data[3];
            String app = (String) data[4];
            Document doc = new Document();
            doc.append(Database.LOGS_TIME, (Long) data[0]);
            doc.append(Database.LOGS_TYPE, type);
            doc.append(Database.LOGS_ADDR, (String) data[2]);
            doc.append(Database.LOGS_TAG, tag);
            doc.append(Database.LOGS_APP, app);
            doc.append(Database.LOGS_CODE, (String) data[5]);
            doc.append(Database.LOGS_TIPS, (String) data[6]);
            doc.append(Database.LOGS_BODY, Util.array2List(data[7]));
            Service.BatchDealer.addDoc(null, doc);

            for (Database.Filter filter : Service.Filters.values()) {
                if (filter.type != null)
                    if (!filter.type.contains(type))
                        continue;
                if (filter.tag != null)
                    if (!filter.tag.contains(tag))
                        continue;
                if (filter.app != null)
                    if (!filter.app.contains(app))
                        continue;
                Service.BatchDealer.addDoc(filter._id, doc);
            }
        } catch (Exception e) {
            System.out.println("[ERROR] rewin.service.log " + e.toString());
        }
    }

    @USEntry(
            tips = "删除日志",
            params = {
                    @USParam(name = "filterId", tips = "过滤器ID，可以为null"),
                    @USParam(name = "query", tips = "查询条件(MongoDB语法)，可以为null")
            },
            readonly = false
    )
    public void clear(ServiceContext ctx, String filterId, Map query) {
        String cname = Database.COL_LOGS + (filterId == null || filterId.trim().isEmpty() ? "" : "_" + filterId.trim());
        MongoCollection col = Service.MongoDBLogs.getCollection(cname);
        col.deleteMany(query == null ? new Document() : new Document(query));
    }

    @USEntry(
            tips = "日志数量查询",
            params = {
                    @USParam(name = "filterId", tips = "过滤器ID，可以为null"),
                    @USParam(name = "query", tips = "查询条件(MongoDB语法)，可以为null")
            },
            result = "日志数量"
    )
    public long count(ServiceContext ctx, String filterId, Map query) {
        String cname = Database.COL_LOGS + (filterId == null || filterId.trim().isEmpty() ? "" : "_" + filterId.trim());
        MongoCollection col = Service.MongoDBLogs.getCollection(cname);
        return col.countDocuments(query == null ? new Document() : new Document(query));
    }

    @USEntry(
            tips = "日志查询",
            params = {
                    @USParam(name = "filterId", tips = "过滤器ID，可以为null"),
                    @USParam(name = "query", tips = "查询条件(MongoDB语法)，可以为null"),
                    @USParam(name = "sort", tips = "排序方式，格式：[\"field\", 1(升序)|-1(降序), ...]，null表示按照time降序"),
                    @USParam(name = "skip", tips = "跳过的记录数"),
                    @USParam(name = "limit", tips = "返回的记录数，0表示不限"),
                    @USParam(name = "fields", tips = "返回的字段(MongoDB语法)，可以为null")
            },
            result = "日志数据列表，每条日志是一个Map"
    )
    public List find(ServiceContext ctx, String filterId, Map query, List sort, int skip, int limit, Map fields) {
        String cname = Database.COL_LOGS + (filterId == null || filterId.trim().isEmpty() ? "" : "_" + filterId.trim());
        MongoCollection col = Service.MongoDBLogs.getCollection(cname);
        return MongoUtil.query(col,
                query == null ? null : new Document(query),
                sort == null ? new Document(Database.LOGS_TIME, -1) : Util.toBson(sort.toArray()),
                skip, limit,
                fields == null ? null : new Document(fields));
    }

    @USEntry(
            tips = "日志的聚合查询",
            params = {
                    @USParam(name = "filterId", tips = "过滤器ID，可以为null"),
                    @USParam(name = "pipeline", tips = "MongoDB aggregate语法的Map列表")
            },
            result = "聚合数据列表，每条数据是一个Map"
    )
    public List aggregate(ServiceContext ctx, String filterId, List pipeline) {
        for ( int i = 0; i < pipeline.size(); i ++ )
            pipeline.set(i, new Document((Map)pipeline.get(i)));
        String cname = Database.COL_LOGS + (filterId == null || filterId.trim().isEmpty() ? "" : "_" + filterId.trim());
        MongoCollection col = Service.MongoDBLogs.getCollection(cname);
        return (List)col.aggregate(pipeline).into(new ArrayList());
    }

    @USEntry(
            tips = "日志的MapReduce查询",
            params = {
                    @USParam(name = "filterId", tips = "过滤器ID，可以为null"),
                    @USParam(name = "map", tips = "MongoDB的map_function"),
                    @USParam(name = "reduce", tips = "MongoDB的reduce_function")
            },
            result = "结果数据列表，每条数据是一个Map"
    )
    public List mapReduce(ServiceContext ctx, String filterId, String map, String reduce) {
        String cname = Database.COL_LOGS + (filterId == null || filterId.trim().isEmpty() ? "" : "_" + filterId.trim());
        MongoCollection col = Service.MongoDBLogs.getCollection(cname);
        return (List)col.mapReduce(map, reduce).into(new ArrayList());
    }

    @USEntry(
            tips = "查询过滤器",
            result = "过滤器数组，每个过滤器是一个Database.Filter结构的Map"
    )
    public Object[] getFilter(ServiceContext ctx) {
        return Service.Filters.values().toArray();
    }

    // 检查过滤器ID是否合法
    static void checkFilterId(String id) throws Exception {
        if ( id == null || id.trim().isEmpty() )
            throw new Exception("invalid filter's _id");
        if ( id.indexOf('.') >= 0 || id.indexOf('$') >= 0 || id.startsWith("system") )
            throw new Exception("invalid filter's _id");
    }

    @USEntry(
            tips = "新建过滤器",
            params = { @USParam(name = "filter", tips = "过滤器，Database.Filter结构的Map") },
            readonly = false
    )
    public void addFilter(ServiceContext ctx, Map filter) throws Exception {
        Database.Filter f = Codec.toType(filter, Database.Filter.class);
        checkFilterId(f._id);
        if ( Service.Filters.containsKey(f._id) )
            throw new Exception("duplicate filter's _id");
        Service.createFilter(f);
    }

    @USEntry(
            tips = "修改过滤器（注：索引即便未改也会被重建）",
            params = { @USParam(name = "filter", tips = "过滤器，Database.Filter结构的Map") },
            timeout = 10,
            readonly = false
    )
    public void setFilter(ServiceContext ctx, Map filter) throws Exception {
        Database.Filter f = Codec.toType(filter, Database.Filter.class);
        if ( !Service.Filters.containsKey(f._id) )
            throw new Exception("filter not found");

        MongoCollection<Database.Filter> col = Service.MongoDBLogs.getCollection(Database.COL_FILTERS, Database.Filter.class);
        col.replaceOne(new Document("_id", f._id), f);

        Service.Filters.put(f._id, f);
        if ( JedisUtil.isInited() )
            JedisUtil.publish(Service.FILTERS_CHANNEL, Service.ListenerID);     // 发出消息通知

        // 重建索引
        String cname = Database.COL_LOGS + "_" + f._id;
        Service.MongoDBLogs.getCollection(cname).dropIndexes();
        Service.createIndex(cname, f.index);
    }

    @USEntry(
            tips = "删除过滤器（注：数据表也会被删除）",
            params = { @USParam(name = "filterId", tips = "过滤器ID") },
            readonly = false
    )
    public void delFilter(ServiceContext ctx, String filterId) throws Exception {
        if ( !Service.Filters.containsKey(filterId) )
            throw new Exception("filter not found");

        Service.BatchDealer.delFilter(filterId);

        MongoCollection<Database.Filter> col = Service.MongoDBLogs.getCollection(Database.COL_FILTERS, Database.Filter.class);
        col.deleteOne(new Document("_id", filterId));

        Service.Filters.remove(filterId);
        if ( JedisUtil.isInited() )
            JedisUtil.publish(Service.FILTERS_CHANNEL, Service.ListenerID);     // 发出消息通知

        // 删除数据表
        String cname = Database.COL_LOGS + "_" + filterId;
        Service.MongoDBLogs.getCollection(cname).drop();
    }

}
