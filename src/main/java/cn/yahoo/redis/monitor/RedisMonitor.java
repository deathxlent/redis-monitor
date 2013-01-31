package cn.yahoo.redis.monitor;

import java.util.Timer;

import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;

/**
 * 任务就是注册临时节点到zk，然后每5s ping一下redis，看看是否存活 zk+redis cluster，fat jar打包 java -jar
 * redis-monitor_fat.jar localhost:2181 /redis/group1 127.0.0.1:6379
 * D:\ws\redis-monitor\log4j1.properties
 *
 * @author guangyi.kou
 */
public class RedisMonitor {
	private static Logger log = LoggerFactory.getLogger(RedisMonitor.class);
	private static String zookeeperConnectionString = "";// zk链接
	private static String path = "";// 路径 /redis/{groupX}
	private static String redis = "";// redis实例
	private static RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,
			3);
	private static CuratorFramework client = null;

	public static void main(String[] args) {
		// 参数请自己保证正确
		if (args == null || args.length != 4) {
			log.error("参数有误！");
			System.exit(0);
		}
		zookeeperConnectionString = args[0];
		path = args[1];
		redis = args[2];
		PropertyConfigurator.configure(args[3]);
		// 1.创建节点√
		log.info("开始创建节点...");
		client = init();
		if (client == null) {
			log.error("zk连接创建有误！");
			System.exit(0);
		}
		log.info("创建节点完毕,开始获取当前状态...");
		try {
			// 5.每隔5s ping一下，没有就exit
			Jedis jedis = new Jedis(getHost(redis), getPort(redis));
			Timer timer = new Timer();
			Task t = new Task(jedis, log, redis);
			timer.schedule(t, 0);
			// TODO 6.每隔1m info一下，查看用量，用量到一定程度，报警
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 1.创建节点
	 *
	 * @return
	 */
	private static CuratorFramework init() {
		try {
			client = CuratorFrameworkFactory.newClient(
					zookeeperConnectionString, retryPolicy);
			client.start();
			client.create().creatingParentsIfNeeded()
					.withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
					.forPath(path + "/redis", redis.getBytes());
			return client;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 得到ip:host字段的ip
	 *
	 * @param redisString
	 * @return
	 */
	private static String getHost(String redisString) {
		return redisString.split(":")[0];
	}

	/**
	 * 得到ip:host字段的port
	 *
	 * @param redisString
	 * @return
	 */
	private static Integer getPort(String redisString) {
		return Integer.valueOf(redisString.split(":")[1]);
	}
}