package cn.yahoo.redis.monitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;

/**
 * zk+redis cluster，fat jar打包 java -jar redis-monitor_fat.jar localhost:2181
 * /redis/group1 127.0.0.1:6379 D:\ws\redis-monitor\log4j1.properties
 *
 * @author guangyi.kou
 */
public class RedisMonitor {
	private static Logger log = LoggerFactory.getLogger(RedisMonitor.class);
	private static String zookeeperConnectionString = "";// zk链接
	private static String path = "";// 路径 /redis/{groupX}
	private static String redis = "";// redis实例
	private static String m_redis = null;// 上级M
	private static RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,
			3);
	private static List<String> redisc = new ArrayList<String>();
	private static CuratorFramework client = null;
	private static Watcher watcher = new Watcher() {
		// watcher
		public void process(WatchedEvent event) {
			System.out.println("event:" + event.getType() + " path:"
					+ event.getPath());
			if (m_redis != null) {// 原来就是M的话就不用管了，直接跳到下面重新watch就行，如果原来不是M的话，就要走下面分支判断了
				// 先得到当前路径的状况
				zkDatas();
				int zkindex = redisc.indexOf(redis);
				if (zkindex == 0) {// M
					// -4.1 发现改变，并且自己是第一个的话，改为M模式
					setRedisM();
				} else {// S
					String tempM = redisc.get(zkindex - 1);// 上一级
					if (!tempM.equals(redis)) {// -4.2发现上一级改变，改slave
						setRedisS();
					}
				}
			}
			// 继续观察
			addWatcher();
		}
	};

	public static void main(String[] args) {
		//参数请自己保证正确
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
		// 2.得到当前路径的状况√
		zkDatas();
		log.info("取当前状态完毕,开始设置自身redis...");
		// 3.设置自身redis
		setRedisStatus();
		log.info("设置自身redis完毕,开始监控zk...");
		// 4.watch zk
		addWatcher();
		log.info("watcher设置完毕,开始不间断ping...");
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
	 * 2.得到当前路径的状况√
	 */
	private static void zkDatas() {
		try {
			List<String> it = client.getChildren().forPath(path);
			Collections.sort(it);
			for (String s : it) {
				redisc.add(new String(client.getData().forPath(path + "/" + s)));
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	/**
	 * 3.设置自身redis
	 */
	private static void setRedisStatus() {
		// -3.1 自己是第一个就是M模式
		int zkindex = redisc.indexOf(redis);
		if (zkindex == 0) {
			// 设置M
			setRedisM();
		} else {// -3.2 不是的话，记录上一级，设自己为S
			m_redis = redisc.get(zkindex - 1);// 记录上一级
			// 设置S
			setRedisS();
		}
	}

	/**
	 * 设置M
	 */
	private static void setRedisM() {
		try {
			Jedis jedis = new Jedis(getHost(redis), getPort(redis));
			jedis.slaveofNoOne();
			jedis.configSet("appendonly", "no");
			jedis.configSet("save", "");
			jedis.disconnect();
			log.info("redis服务器 " + redis + "设为主");
		} catch (Exception e) {
			log.error("redis服务器 " + redis + " 失去连接");
			System.exit(1);
		}
	}

	/**
	 * 设置S
	 */
	private static void setRedisS() {
		try {
			Jedis jedis = new Jedis(getHost(redis), getPort(redis));
			jedis.slaveof(getHost(m_redis), getPort(m_redis));
			jedis.configSet("appendonly", "yes");
			// TODO 需要实际算一下规则
			jedis.configSet("appendfsync", "everysec");
			jedis.configSet("save", "900 1 300 10 60 10000");
			jedis.disconnect();
			log.info("redis服务器 " + redis + "设为" + m_redis + "的从");
		} catch (Exception e) {
			log.error("redis服务器 " + redis + " 失去连接");
			System.exit(1);
		}
	}

	/**
	 * 添加watcher
	 */
	private static void addWatcher() {
		try {
			client.getChildren().usingWatcher(watcher).forPath(path);
		} catch (Exception e) {
			e.printStackTrace();
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

class Task extends TimerTask {
	private Jedis jedis;
	private Logger log;
	private String redis;

	Task(Jedis jedis, Logger log, String redis) {
		this.jedis = jedis;
		this.log = log;
		this.redis = redis;
	}

	public void run() {
		log.info("ping " + redis + "...");
		try {
			if (!jedis.ping().equalsIgnoreCase("pong")) {
				log.error("redis服务器 " + redis + " 失去连接");
				System.exit(1);
			}
		} catch (Exception e) {
			log.error("redis服务器 " + redis + " 失去连接");
			System.exit(1);
		}
		log.info("pong");
		Timer timer = new Timer();
		Task t = new Task(jedis, log, redis);
		timer.schedule(t, 5 * 1000);
	}
}
