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
 * zk+redis cluster��fat jar��� java -jar redis-monitor_fat.jar localhost:2181
 * /redis/group1 127.0.0.1:6379 D:\ws\redis-monitor\log4j1.properties
 *
 * @author guangyi.kou
 */
public class RedisMonitor {
	private static Logger log = LoggerFactory.getLogger(RedisMonitor.class);
	private static String zookeeperConnectionString = "";// zk����
	private static String path = "";// ·�� /redis/{groupX}
	private static String redis = "";// redisʵ��
	private static String m_redis = null;// �ϼ�M
	private static RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,
			3);
	private static List<String> redisc = new ArrayList<String>();
	private static CuratorFramework client = null;
	private static Watcher watcher = new Watcher() {
		// watcher
		public void process(WatchedEvent event) {
			System.out.println("event:" + event.getType() + " path:"
					+ event.getPath());
			if (m_redis != null) {// ԭ������M�Ļ��Ͳ��ù��ˣ�ֱ��������������watch���У����ԭ������M�Ļ�����Ҫ�������֧�ж���
				// �ȵõ���ǰ·����״��
				zkDatas();
				int zkindex = redisc.indexOf(redis);
				if (zkindex == 0) {// M
					// -4.1 ���ָı䣬�����Լ��ǵ�һ���Ļ�����ΪMģʽ
					setRedisM();
				} else {// S
					String tempM = redisc.get(zkindex - 1);// ��һ��
					if (!tempM.equals(redis)) {// -4.2������һ���ı䣬��slave
						setRedisS();
					}
				}
			}
			// �����۲�
			addWatcher();
		}
	};

	public static void main(String[] args) {
		//�������Լ���֤��ȷ
		if (args == null || args.length != 4) {
			log.error("��������");
			System.exit(0);
		}
		zookeeperConnectionString = args[0];
		path = args[1];
		redis = args[2];
		PropertyConfigurator.configure(args[3]);
		// 1.�����ڵ��
		log.info("��ʼ�����ڵ�...");
		client = init();
		if (client == null) {
			log.error("zk���Ӵ�������");
			System.exit(0);
		}
		log.info("�����ڵ����,��ʼ��ȡ��ǰ״̬...");
		// 2.�õ���ǰ·����״����
		zkDatas();
		log.info("ȡ��ǰ״̬���,��ʼ��������redis...");
		// 3.��������redis
		setRedisStatus();
		log.info("��������redis���,��ʼ���zk...");
		// 4.watch zk
		addWatcher();
		log.info("watcher�������,��ʼ�����ping...");
		try {
			// 5.ÿ��5s pingһ�£�û�о�exit
			Jedis jedis = new Jedis(getHost(redis), getPort(redis));
			Timer timer = new Timer();
			Task t = new Task(jedis, log, redis);
			timer.schedule(t, 0);
			// TODO 6.ÿ��1m infoһ�£��鿴������������һ���̶ȣ�����
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 1.�����ڵ�
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
	 * 2.�õ���ǰ·����״����
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
	 * 3.��������redis
	 */
	private static void setRedisStatus() {
		// -3.1 �Լ��ǵ�һ������Mģʽ
		int zkindex = redisc.indexOf(redis);
		if (zkindex == 0) {
			// ����M
			setRedisM();
		} else {// -3.2 ���ǵĻ�����¼��һ�������Լ�ΪS
			m_redis = redisc.get(zkindex - 1);// ��¼��һ��
			// ����S
			setRedisS();
		}
	}

	/**
	 * ����M
	 */
	private static void setRedisM() {
		try {
			Jedis jedis = new Jedis(getHost(redis), getPort(redis));
			jedis.slaveofNoOne();
			jedis.configSet("appendonly", "no");
			jedis.configSet("save", "");
			jedis.disconnect();
			log.info("redis������ " + redis + "��Ϊ��");
		} catch (Exception e) {
			log.error("redis������ " + redis + " ʧȥ����");
			System.exit(1);
		}
	}

	/**
	 * ����S
	 */
	private static void setRedisS() {
		try {
			Jedis jedis = new Jedis(getHost(redis), getPort(redis));
			jedis.slaveof(getHost(m_redis), getPort(m_redis));
			jedis.configSet("appendonly", "yes");
			// TODO ��Ҫʵ����һ�¹���
			jedis.configSet("appendfsync", "everysec");
			jedis.configSet("save", "900 1 300 10 60 10000");
			jedis.disconnect();
			log.info("redis������ " + redis + "��Ϊ" + m_redis + "�Ĵ�");
		} catch (Exception e) {
			log.error("redis������ " + redis + " ʧȥ����");
			System.exit(1);
		}
	}

	/**
	 * ���watcher
	 */
	private static void addWatcher() {
		try {
			client.getChildren().usingWatcher(watcher).forPath(path);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * �õ�ip:host�ֶε�ip
	 *
	 * @param redisString
	 * @return
	 */
	private static String getHost(String redisString) {
		return redisString.split(":")[0];
	}

	/**
	 * �õ�ip:host�ֶε�port
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
				log.error("redis������ " + redis + " ʧȥ����");
				System.exit(1);
			}
		} catch (Exception e) {
			log.error("redis������ " + redis + " ʧȥ����");
			System.exit(1);
		}
		log.info("pong");
		Timer timer = new Timer();
		Task t = new Task(jedis, log, redis);
		timer.schedule(t, 5 * 1000);
	}
}
