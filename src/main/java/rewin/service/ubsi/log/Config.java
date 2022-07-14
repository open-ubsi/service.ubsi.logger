package rewin.service.ubsi.log;

import rewin.ubsi.common.MongoUtil;

import java.util.List;

/**
 * 配置信息
 */
public class Config {

    public List<MongoUtil.Server>   mongo_servers;              // 服务器列表
    public List<MongoUtil.Server>   mongo_servers_restart;      // 服务器列表（重启后生效）
    public String                   mongo_servers_comment = MongoUtil.ServerComment;        // 服务器列表说明

    public List<MongoUtil.Auth>     mongo_auth;                 // 用户认证
    public List<MongoUtil.Auth>     mongo_auth_restart;         // 用户认证（重启后生效）
    public String                   mongo_auth_comment = MongoUtil.AuthComment;             // 用户认证说明

    public MongoUtil.Option         mongo_option;               // 配置项
    public MongoUtil.Option         mongo_option_restart;       // 配置项（重启后生效）
    public MongoUtil.OptionComment  mongo_option_comment = new MongoUtil.OptionComment();   // 配置项说明

}
