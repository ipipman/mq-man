# mq-man
从零开始,手写一个MQ框架

![img.png](https://ipman-blog-1304583208.cos.ap-nanjing.myqcloud.com/mq%2F511719635282_.pic_hd.jpg)

第一个版本-内存Queue 
- 基于内存 Queue 实现生产和消费API
- 创建内存 BlockingQueue, 作为底层消息存储
- 定义 Topic ,支持多个Topic
- 定义 Producer,支持Send消息
- 定义 Consumer,支持Poll消息

第二个版本: 自定义Queue
去掉内存 Queue, 设计自定义 Queue,实现消息确认和消费 offset
- 自定义内存Message 数组模拟 Queue
- 使用指针记录当前消息写入位置
- 对于每个命令消费者,用指针记录消费位置

第三个版本:基于 SpringMVC 实现 MQServer
- 拆分 broker 和 client (包括 producer 和 consumer)
- 设计消息读写 API 接口,确认接口,提交 offset 接口
- 设计消息读写API接口,确认接口,提交offset接口
- producer 和 consumer 通过 httpclient 访问 Queue
- 实现消息确认, offset提交
- 实现 consumer 从 offset 增量拉取

第四个版本: 功能完善MQ
增加多种策略(各条之间没有关系, 可以任意选择实现)
- 考虑实现消息过期,消息重试,消息定时投递等策略
- 考虑批量操作, 包括读写, 可以打包和压缩
- 考虑消息清理策略, 包括读写,可以打包和压缩
- 考虑消息清理策略, 包括定时清理,按容量清理、LRU 等
- 考虑消息持久化,存入数据库, 或 WAL 日志文件,或 BookKeeper
- 考虑 Spring mvc 替换成 netty 的TCP 协议, rsocket/websocket

第五个版本: 体系完善MQ
对接各种技术 (各条之间没有关系,可以任意选择实现)
- 考虑封装 JMS 1.1 接口规范
- 考虑实现 STOMP 消息规范
- 考虑实现消息事务机制与事务管理器
- 对接Spring
- 对接 Camel 或Spring Integration
- 优化内存和磁盘使用

