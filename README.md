#Gearman Java Implementation
对Gearman server，client，work的java实现
功能有：
*   存储引擎支持Mysql，Redis
*   支持Cron的任务提交
*   Metrics监控
*   支持原生的Gearman协议，未对协议进行破坏

##如何使用
* Server:直接运行Main函数 net.github.gearman.server.GearmanDaemon
* Work: 见gexample下的WorkDemo
* Client: 见gexample下的ClientDemo


##监控Console
待完成