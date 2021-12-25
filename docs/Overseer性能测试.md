# Overseer 单点 性能测试报告

## 测试环境

- CPU：Intel Core i7-8750H 2.20GHz
- 内存：16GB
- Java：JDK 8
- 集群：一台Overseer服务器，两台Storage服务器，内网环境带宽不计。

## 测试报告

分别测试Overseer的三种功能：元数据上传、元数据获取、元数据删除

### 元数据上传

测试代码如下，一共开启1000个并发线程，每个线程上传相同文件的元数据100次，总任务量10w。

为了测试精确度，这里将客户端的MD5计算关闭，减少客户端操作对Overseer性能测试影响。

```java
	public static void main(String[] args) throws Exception {
        Config config = new Config(args[0]);
        SwarmClient client = new SwarmClient(config);


        
        // 1000 个线程并发
        int threadCount = 1000;
        // 每个线程任务数：100
        int task = 100;
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        // 开始时间
        long start = System.currentTimeMillis();
        for(int i = 0; i < threadCount; i++){
            Thread thread = new Thread(() -> {
                for(int j = 0; j < count; j++){
                    try {
                        // 上传元数据
                        client.uploadHelper.uploadMeta("D:/log.log", 1);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                countDownLatch.countDown();
            });
            thread.start();
        }
        countDownLatch.await();
        // 总用时
        long used = System.currentTimeMillis() - start;
        log.info("upload meta time used: {} ms, TPS: {}", used, threadCount * count * 1000 / used);
        client.shutdownGracefully();
    }
```

测试结果如下：

```
2021-12-25 11:43:06,063 [main] INFO - upload meta time used: 4554 ms, TPS: 21958
2021-12-25 11:44:50,338 [main] INFO - upload meta time used: 4731 ms, TPS: 21137
2021-12-25 11:45:20,590 [main] INFO - upload meta time used: 4116 ms, TPS: 24295
```

1000个并发线程，总上传量10W，平均用时4.4s，**TPS约为 2.2万**。

### 元数据拉取

测试代码如下，一共1000个并发线程，每个线程拉取同一个ID的元数据100次，共计10w次请求。

```java
	public static void main(String[] args) throws Exception {
        Config config = new Config(args[0]);
        SwarmClient client = new SwarmClient(config);

        // 1000 个线程并发
        int threadCount = 1000;
        // 每个线程任务数：100
        int task = 100;
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        // 开始时间
        long start = System.currentTimeMillis();
        for(int i = 0; i < threadCount; i++){
            Thread thread = new Thread(() -> {
                for(int j = 0; j < task; j++){
                    try {
                        // 拉取元数据
                        DownloadResponse response = client.downloadHelper.sendDownloadRequest("4221615c-043d-4abc-af5c-676317e39d1d");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                countDownLatch.countDown();
            });
            thread.start();
        }
        countDownLatch.await();
        // 总用时
        long used = System.currentTimeMillis() - start;
        log.info("pull meta time used: {} ms, TPS: {}", used, threadCount * task * 1000 / used);
        client.shutdownGracefully();
    }
```

三次测试结果如下：

```
2021-12-25 11:30:14,923 [main] INFO  - pull meta time used: 2021 ms, TPS: 49480
2021-12-25 11:37:31,719 [main] INFO  - pull meta time used: 1829 ms, TPS: 54674
2021-12-25 11:38:11,276 [main] INFO  - pull meta time used: 1972 ms, TPS: 50709
```

10W次拉取请求，平均用时约为2s，**TPS约等于 5万**

## 总结

目前测试结果看来，单点的Overseer写入性能能够达到 2 万TPS，查询性能能够达到 5万TPS。

在目前的主从结构下，集群化的Overseer能够形成强大的读能力。但是集群中只有一台主节点具有写入能力，这会导致整个系统的写性能下降。目前还在寻找一种更优秀的架构来解决该问题。