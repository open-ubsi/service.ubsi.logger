package rewin.service.ubsi.log;

import com.mongodb.WriteConcern;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 日志数据的批量写入
 */
public class BatchDeal extends Thread {

    ConcurrentLinkedQueue<Document> Buffer = new ConcurrentLinkedQueue<>();                     // 主表
    ConcurrentMap<String, ConcurrentLinkedQueue<Document>> FBuffer = new ConcurrentHashMap<>(); // 过滤表

    boolean         Shutdown = false;
    List<Document>  WBuffer = new ArrayList<>();    // 写入缓冲区
    AtomicLong      Count = new AtomicLong(0);      // 日志计数

    void batch(ConcurrentLinkedQueue<Document> buf, String cname) {
        while ( !buf.isEmpty() )
            WBuffer.add(buf.poll());
        if ( !WBuffer.isEmpty() ) {
            try {
                Service.MongoDBLogs.getCollection(cname)
                        .withWriteConcern(WriteConcern.UNACKNOWLEDGED).insertMany(WBuffer);
            } catch (Exception e) {
                // 记录日志发生错误，只在console输出异常信息
                System.out.println("[ERROR] rewin.service.log " + e.toString());
            }
            WBuffer.clear();
        }
    }

    /** 线程开始 */
    public void run() {
        while ( !Shutdown ) {
            synchronized (this) {
                try { this.wait(100); } catch (Exception e) { }   // 等待100毫秒
            }
            Count.addAndGet((long)Buffer.size());
            batch(Buffer, Database.COL_LOGS);
            for ( String key : FBuffer.keySet() ) {
                ConcurrentLinkedQueue<Document> buffer = FBuffer.get(key);
                if ( buffer != null )
                    batch(buffer, Database.COL_LOGS + "_" + key);
            }
        }
    }

    /** 结束 */
    public void close() {
        Shutdown = true;
        wakeup();
        try { join(); } catch (Exception e) {}      // 等待线程结束
        Buffer.clear();
        FBuffer.clear();
    }

    /** 唤醒线程 */
    public void wakeup() {
        synchronized (this) { notifyAll(); }
    }

    /** 新增记录 */
    public void addDoc(String filter, Document doc) {
        ConcurrentLinkedQueue<Document> buf = filter == null ? Buffer : FBuffer.get(filter);
        if ( buf != null ) {
            buf.offer(doc);
            if ( buf.size() >= 100 )
                wakeup();
        }
    }
    /** 新增Filter */
    public void addFilter(String filter) {
        FBuffer.put(filter, new ConcurrentLinkedQueue<>());
    }
    /** 删除Filter */
    public void delFilter(String filter) {
        FBuffer.remove(filter);
    }
    /** 清除所有Filter */
    public void clearFilter() {
        FBuffer.clear();
    }
}
