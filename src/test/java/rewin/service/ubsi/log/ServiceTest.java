package rewin.service.ubsi.log;

import org.junit.Test;
import org.junit.Before; 
import org.junit.After;
import rewin.ubsi.cli.Request;
import rewin.ubsi.common.Codec;
import rewin.ubsi.common.LogUtil;
import rewin.ubsi.common.Util;
import rewin.ubsi.consumer.Context;
import rewin.ubsi.consumer.LogBody;
import rewin.ubsi.container.Bootstrap;
import rewin.ubsi.container.Info;

import java.util.Arrays;

public class ServiceTest {

String ServiceName = "rewin.ubsi.logger";               // 服务名字
String ClassName = "rewin.service.ubsi.log.Service";    // 类名字

@Before
public void before() throws Exception {
    Bootstrap.start();
} 

@After
public void after() throws Exception {
    Bootstrap.stop();
}

@Test
public void install() throws Exception {
    // 检查模块是否安装
    Context context = Context.request("", "getRuntime", null);
    Object res = context.direct("localhost", 7112);
    Info.Runtime runtime = Codec.toType(res, Info.Runtime.class);
    if ( !runtime.services.containsKey(ServiceName) ) {
        // 安装模块
        context = Context.request("", "install", ServiceName, ClassName, null);
        context.direct("localhost", 7112);
    }
    // 启动模块
    context = Context.request("", "setStatus", ServiceName, 1);
    context.direct("localhost", 7112);
}

@Test
public void testGetConfig() throws Exception {
    Context context = Context.request("", "getConfig", ServiceName);
    Object res = context.direct("localhost", 7112);
    Request.printJson(res);
}

@Test
public void testSetConfig() throws Exception {
    String config = "{" +
            "   \"mongo_servers\": [ { \"host\":\"192.168.1.116\", \"port\":27017 } ]" +
            "}";
    Context context = Context.request("", "setConfig", ServiceName, config);
    Object res = context.direct("localhost", 7112);
    Request.printJson(res);
}

@Test
public void testInfo() throws Exception {
    Context context = Context.request("", "getRuntime", ServiceName);
    Object res = context.direct("localhost", 7112);
    Request.printJson(res);
}

@Test
public void testLog() throws Exception {
    Context context = Context.request(ServiceName, "log", new Object[] {
            System.currentTimeMillis(),
            LogUtil.ACCESS,
            "localhost",
            "rewin.ubsi.service",
            Context.LOG_APPID,
            null,
            "test log",
            new LogBody.Request("reqId", "seqId", "service", "entry", (byte)0)
    });
    Object res = context.direct("localhost", 7112);
    Request.printJson(res);
}

@Test
public void testCount() throws Exception {
    Context context = Context.request(ServiceName, "count", Service.FilterUbsiAccess._id, null);
    Object res = context.direct("localhost", 7112);
    Request.printJson(res);
}


@Test
public void testFind() throws Exception {
    Context context = Context.request(ServiceName, "find", Service.FilterUbsiAccess._id, null, null, 0, 0, null);
    Object res = context.direct("localhost", 7112);
    Request.printJson(res);
}

@Test
public void testAggregate() throws Exception {
    Context context = Context.request(ServiceName, "aggregate", null, Arrays.asList(
            Util.toMap(new Object[] {
                    "$group", Util.toMap(new Object[] {
                        "_id", "type", "num", Util.toMap(new Object[] { "$sum", 1 })
                    })
            })
    ));
    Object res = context.direct("localhost", 7112);
    Request.printJson(res);
}

@Test
public void testGetFiltere() throws Exception {
    Context context = Context.request(ServiceName, "getFilter");
    Object res = context.direct("localhost", 7112);
    Request.printJson(res);
}

@Test
public void testAddFilter() throws Exception {
    Database.Filter filter = new Database.Filter();
    filter._id = "test";
    filter.type = Arrays.asList(LogUtil.ACCESS);
    filter.index = Arrays.asList(
            Arrays.asList(new Database.Index("tips", 0))
    );
    Context context = Context.request(ServiceName, "addFilter", filter);
    Object res = context.direct("localhost", 7112);
    Request.printJson(res);
}

@Test
public void testSetFilter() throws Exception {
    Database.Filter filter = new Database.Filter();
    filter._id = "test";
    filter.type = Arrays.asList(LogUtil.ACCESS);
    filter.index = Arrays.asList(
            Arrays.asList(new Database.Index("app", 0))
    );
    Context context = Context.request(ServiceName, "setFilter", filter);
    Object res = context.direct("localhost", 7112);
    Request.printJson(res);
}

@Test
public void testDelFilter() throws Exception {
    Context context = Context.request(ServiceName, "delFilter", "test");
    Object res = context.direct("localhost", 7112);
    Request.printJson(res);
}

}
