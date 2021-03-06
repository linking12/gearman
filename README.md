#Gearman介绍
它是分布式的程序调用框架，可完成跨语言的相互调用，适合在后台运行工作任务。最初是2005年perl版本，2008年发布C/C++版本。目前大部分源码都是（Gearmand服务job Server）C++，各个API实现有各种语言的版本。PHP的Client API与Worker API实现为C扩展，在PHP官方网站有此扩展的中英文文档。
##gearman架构中的三个角色
client：请求的发起者，工作任务的需求方（可以是C、PHP、Java、Perl、Mysql udf等等）

Job Server：请求的调度者，负责将client的请求转发给相应的worker（gearmand服务进程创建）

worker：请求的处理者（可以是C、PHP、Java、Perl等等）
##gearman是如何工作的
![工作原理](https://github.com/linking12/gearman/blob/master/%E5%B7%A5%E4%BD%9C%E5%8E%9F%E7%90%86.png "原理")

Gearman Client API，Gearman Worker API，Gearman Job Server都是由gearman本身提供，我们在应用中只需要调用即可。目前client与worker api都很丰富。

#Gearman Java 实现
由于Gearman的JobServer不支持Cron任务，且Client Api和 Worker Api比较冗余且复杂，故而对Gearman server，client，work使用java进行重新实现
功能有：
*   存储引擎支持Mysql，Redis（主要支持Mysql）
*   支持Cron的任务提交
*   Metrics监控
*   支持原生的Gearman协议，未对协议进行破坏
*   Cron任务使用quartz来进行调度，且每一次调度都会进行一次持久化，保证每一次调度不会丢失，确保一定执行
*   worker使用拉的方式进行获取任务，对Job Server无性能影响

##如何使用
* Server:直接运行Main net.github.gearman.server.GearmanDaemon
* Work: 见gexample下的WorkDemo
* Client: 见gexample下的ClientDemo


##监控Console
提供RestApi 
* http://localhost:8080/dashboard/totalData 整个报表数据
* http://localhost:8080/dashboard/queues  任务调度数据
