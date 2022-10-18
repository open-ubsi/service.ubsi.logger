# service.ubsi.logger
微服务：UBSI日志（社区版）
===
```
服务类：rewin.service.ubsi.log.Service
服务名字：rewin.ubsi.logger
数据库配置：MongoDB
```
将分布式环境下的UBSI应用（微服务/消费者）产生的日志统一收集保存在MongoDB中，并为UBSI治理工具提供查询/统计接口。
> 注：不要在项目中依赖微服务的JAR包。