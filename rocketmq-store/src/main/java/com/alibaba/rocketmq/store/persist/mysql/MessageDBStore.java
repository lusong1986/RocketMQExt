package com.alibaba.rocketmq.store.persist.mysql;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.store.persist.MsgStore;

public class MessageDBStore implements MsgStore {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

	private MessageDBStoreConfig messageDBStoreConfig;

	private Connection connection;

	private AtomicLong totalRecordsValue = new AtomicLong(0);

	private SimpleDateFormat myFmt = new SimpleDateFormat("yyyyMMdd");

	private SimpleDateFormat myFmt2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public MessageDBStore(MessageDBStoreConfig messageDBStoreConfig) {
		this.messageDBStoreConfig = messageDBStoreConfig;
	}

	private boolean loadDriver() {
		try {
			Class.forName(this.messageDBStoreConfig.getJdbcDriverClass()).newInstance();
			log.info("Loaded the appropriate driver, {}", this.messageDBStoreConfig.getJdbcDriverClass());
			return true;
		} catch (Exception e) {
			log.error("Loaded the appropriate driver Exception", e);
		}

		return false;
	}

	public boolean open() {
		if (this.loadDriver()) {
			Properties props = new Properties();
			props.put("user", messageDBStoreConfig.getJdbcUser());
			props.put("password", messageDBStoreConfig.getJdbcPassword());

			try {
				this.connection = DriverManager.getConnection(this.messageDBStoreConfig.getJdbcURL(), props);

				this.connection.setAutoCommit(false);

				return true;
			} catch (SQLException e) {
				log.error("Create JDBC Connection Exeption:" + e.getMessage(), e);
			}
		}

		return false;
	}

	public void close() {
		try {
			if (this.connection != null) {
				this.connection.close();
			}
		} catch (SQLException e) {
			log.error("close JDBC Connection Exeption", e);
		}
	}

	@Override
	public boolean store(List<MessageExt> msgs) {
		if (null == msgs || msgs.size() == 0) {
			log.warn(">>>>>>>>msgs is empty.");
			return false;
		}

		if (null == this.connection) {
			log.warn(">>>>>>>>jdbc connection is null.");
			return false;
		}

		PreparedStatement statement = null;
		try {
			this.connection.setAutoCommit(false);

			Map<String, Object> params = new LinkedHashMap<String, Object>();
			for (MessageExt messageExt : msgs) {
				params.put("msgId", messageExt.getMsgId());
				params.put("queueID", messageExt.getQueueId());
				params.put("queueOffset", messageExt.getQueueOffset());
				params.put("commitLogOffset", messageExt.getCommitLogOffset());
				params.put("sysflag", messageExt.getSysFlag());
				params.put("reconsumeTimes", messageExt.getReconsumeTimes());
				params.put("flag", messageExt.getFlag());
				params.put("storeSize", messageExt.getStoreSize());
				params.put("bodyCRC", messageExt.getBodyCRC());
				params.put("preparedTransactionOffset", messageExt.getPreparedTransactionOffset());
				params.put("storeHost", getHostString(messageExt.getStoreHost()));
				params.put("bornHost", getHostString(messageExt.getBornHost()));
				params.put("topic", messageExt.getTopic());
				params.put("tags", messageExt.getTags() == null ? "" : messageExt.getTags());
				params.put("indexkeys", messageExt.getKeys() == null ? "" : messageExt.getKeys());

				String bodyContentStr = "";
				try {
					bodyContentStr = new String(messageExt.getBody(), "utf-8");
				} catch (Throwable e) {
					log.warn("failed to convert text-based Message content:{}" + e.getMessage(), messageExt.getMsgId());
				}
				params.put("content", bodyContentStr);
				params.put("properties", JSON.toJSONString(messageExt.getProperties()));
				params.put("bornTime", myFmt2.format(new Date(messageExt.getBornTimestamp())));
				params.put("storeTime", myFmt2.format(new Date(messageExt.getStoreTimestamp())));

				// TODO: 在凌晨00:00左右时 有可能存错数据库。但为了批量插入数据库，先批量插入
				if (null == statement) {
					String tableName = "messages_" + myFmt.format(new Date(messageExt.getStoreTimestamp()));
					statement = this.connection.prepareStatement(getPrepareSql(params, tableName));
				}

				int index = 1;
				for (Entry<String, Object> param : params.entrySet()) {
					if (param.getValue() != null) {
						statement.setObject(index, param.getValue());
					}
					index++;
				}
				statement.addBatch();
			}

			int[] updateCounts = statement.executeBatch();
			this.connection.commit();

			if (updateCounts != null && updateCounts.length > 0) {
				for (int updateCount : updateCounts) {
					this.totalRecordsValue.addAndGet(updateCount);
				}
			}

			return true;
		} catch (Exception e) {
			log.warn("store messageExt Exception", e);
		} finally {
			if (null != statement) {
				try {
					statement.close();
				} catch (SQLException e) {
				}
			}
		}

		return false;
	}

	private String getPrepareSql(Map<String, Object> params, String tableName) {
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ");
		sb.append(tableName + "( ");

		for (Entry<String, Object> param : params.entrySet()) {
			sb.append(param.getKey());
			sb.append(",");
		}
		sb.deleteCharAt(sb.length() - 1);

		sb.append(" ) values( ");

		for (int i = 0; i < params.size(); i++) {
			sb.append("?");
			sb.append(",");
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append(" ) ");
		return sb.toString();
	}

	private String getHostString(SocketAddress host) {
		if (host != null) {
			InetSocketAddress inetSocketAddress = (InetSocketAddress) host;
			return inetSocketAddress.getAddress().getHostAddress() + ":" + inetSocketAddress.getPort();
		}

		return "";
	}

	public MessageDBStoreConfig getMessageDBStoreConfig() {
		return messageDBStoreConfig;
	}

	public void setMessageDBStoreConfig(MessageDBStoreConfig messageDBStoreConfig) {
		this.messageDBStoreConfig = messageDBStoreConfig;
	}

}
