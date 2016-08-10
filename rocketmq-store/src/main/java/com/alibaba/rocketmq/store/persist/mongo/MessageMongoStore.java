package com.alibaba.rocketmq.store.persist.mongo;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.store.persist.MsgStore;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public class MessageMongoStore implements MsgStore {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

	private MessageMongoStoreConfig messageMongoStoreConfig;

	private MongoClient mgClient;

	private DB mqDb;

	private AtomicLong totalRecordsValue = new AtomicLong(0);

	private final SimpleDateFormat myFmt = new SimpleDateFormat("yyyyMMdd");

	private final SimpleDateFormat myFmt2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public MessageMongoStore(MessageMongoStoreConfig messageMongoStoreConfig) {
		this.messageMongoStoreConfig = messageMongoStoreConfig;
	}

	public boolean open() {
		if (null == messageMongoStoreConfig.getMongoRepSetHosts()
				|| messageMongoStoreConfig.getMongoRepSetHosts().isEmpty()) {
			return false;
		}

		if (null == messageMongoStoreConfig.getMongoDbName() || messageMongoStoreConfig.getMongoDbName().isEmpty()) {
			return false;
		}

		try {
			List<ServerAddress> addresses = new ArrayList<ServerAddress>();
			String[] mongoHosts = messageMongoStoreConfig.getMongoRepSetHosts().trim().split(",");
			if (mongoHosts != null && mongoHosts.length > 0) {
				for (String mongoHost : mongoHosts) {
					if (mongoHost != null && mongoHost.length() > 0) {
						String[] mongoServer = mongoHost.split(":");
						if (mongoServer != null && mongoServer.length == 2) {
							log.info(">>>>>>>>add mongo server>>" + mongoServer[0] + ":" + mongoServer[1]);
							ServerAddress address = new ServerAddress(mongoServer[0].trim(),
									Integer.parseInt(mongoServer[1].trim()));
							addresses.add(address);
						}
					}
				}
			}

			// Caused by: java.security.NoSuchAlgorithmException: PBKDF2WithHmacSHA1 SecretKeyFactory not available,need
			// jre/lib/ext
			List<MongoCredential> credentialsList = new LinkedList<MongoCredential>();
			MongoCredential credential = MongoCredential.createCredential(messageMongoStoreConfig.getMongoUser(),
					messageMongoStoreConfig.getMongoDbName(), messageMongoStoreConfig.getMongoPassword().toCharArray());
			credentialsList.add(credential);
			mgClient = new MongoClient(addresses, credentialsList);
			mqDb = mgClient.getDB(messageMongoStoreConfig.getMongoDbName());

			log.info(">>>>>>>>>open mongodb successfully.");
			return true;
		} catch (Throwable e) {
			log.error("open mongo Exeption " + e.getMessage(), e);
		}

		return false;
	}

	public void close() {
		try {
			if (this.mgClient != null) {
				this.mgClient.close();
			}
		} catch (Throwable e) {
			log.error("close mongo Exeption", e);
		}
	}

	@Override
	public boolean store(List<MessageExt> msgs) {
		if (null == msgs || msgs.size() == 0) {
			log.warn(">>>>>>>>msgs is empty.");
			return false;
		}

		if (null == this.mqDb) {
			log.warn(">>>>>>>>mqDb is null.");
			return false;
		}

		try {
			for (MessageExt messageExt : msgs) {
				DBCollection mqMessageCollection = mqDb.getCollection("messages_"
						+ myFmt.format(new Date(messageExt.getStoreTimestamp())));

				MongoMessage mongoMessage = new MongoMessage();
				mongoMessage.setQueueId(messageExt.getQueueId());
				mongoMessage.setStoreSize(messageExt.getStoreSize());
				mongoMessage.setQueueOffset(messageExt.getQueueOffset());
				mongoMessage.setSysFlag(messageExt.getSysFlag());
				mongoMessage.setStoreTime(myFmt2.format(new Date(messageExt.getStoreTimestamp())));
				mongoMessage.setBornTime(myFmt2.format(new Date(messageExt.getBornTimestamp())));
				mongoMessage.setBornHost(getHostString(messageExt.getBornHost()));
				mongoMessage.setStoreHost(getHostString(messageExt.getStoreHost()));

				mongoMessage.setMsgId(messageExt.getMsgId());
				mongoMessage.setCommitLogOffset(messageExt.getCommitLogOffset());
				mongoMessage.setBodyCRC(messageExt.getBodyCRC());
				mongoMessage.setReconsumeTimes(messageExt.getReconsumeTimes());
				mongoMessage.setPreparedTransactionOffset(messageExt.getPreparedTransactionOffset());
				mongoMessage.setTopic(messageExt.getTopic());
				mongoMessage.setFlag(messageExt.getFlag());
				mongoMessage.setTags(messageExt.getTags() == null ? "" : messageExt.getTags());
				mongoMessage.setKeys(messageExt.getKeys() == null ? "" : messageExt.getKeys());

				final Map<String, String> msgProperties = messageExt.getProperties();
				String _catChildMessageId1 = msgProperties.get("_catChildMessageId1");
				if (null == _catChildMessageId1) {
					_catChildMessageId1 = "";
				}
				mongoMessage.set_catChildMessageId1(_catChildMessageId1);

				String _catParentMessageId = msgProperties.get("_catParentMessageId");
				if (null == _catParentMessageId) {
					_catParentMessageId = "";
				}
				mongoMessage.set_catParentMessageId(_catParentMessageId);

				String _catParentMessageId1 = msgProperties.get("_catParentMessageId1");
				if (null == _catParentMessageId1) {
					_catParentMessageId1 = "";
				}
				mongoMessage.set_catParentMessageId1(_catParentMessageId1);

				String _catRootMessageId = msgProperties.get("_catRootMessageId");
				if (null == _catRootMessageId) {
					_catRootMessageId = "";
				}
				mongoMessage.set_catRootMessageId(_catRootMessageId);

				String bodyContentStr = "";
				try {
					bodyContentStr = new String(messageExt.getBody(), "utf-8");
				} catch (Throwable e) {
					log.warn("failed to convert text-based Message content:{}" + e.getMessage(), messageExt.getMsgId());
				}
				mongoMessage.setContent(bodyContentStr);

				mongoMessage.setPropertiesString(JSON.toJSONString(msgProperties));

				try {
					DBObject dbObject = BasicDBObjectUtils.castModel2DBObject(mongoMessage);
					mqMessageCollection.insert(dbObject);

					this.totalRecordsValue.addAndGet(1);

					if (messageExt.getPreparedTransactionOffset() > 0) {
						final int tranType = MessageSysFlag.getTransactionValue(messageExt.getSysFlag());
						int tranStatus = 0;
						if (tranType == MessageSysFlag.TransactionCommitType) {
							tranStatus = 1;// commit
						} else if (tranType == MessageSysFlag.TransactionRollbackType) {
							tranStatus = 2;// rollback
						}

						if (tranStatus > 0) {
							final DBObject query = new BasicDBObject();
							query.put("commitLogOffset", messageExt.getPreparedTransactionOffset());
							DBObject retObject = mqMessageCollection.findOne(query);
							if (retObject != null) {
								retObject.put("tranStatus", tranStatus);
								mqMessageCollection.update(query, retObject);
								log.info(">>>>>>>>>>>>>>update prepare message: " + retObject);
							}
						}

					}

				} catch (Exception e) {
					log.warn("insert mongo error:" + e.getMessage(), e);
				}
			}

			return true;
		} catch (Throwable e) {
			log.warn("mongo store messageExt Exception", e);
		}

		return false;
	}

	private String getHostString(SocketAddress host) {
		if (host != null) {
			InetSocketAddress inetSocketAddress = (InetSocketAddress) host;
			return inetSocketAddress.getAddress().getHostAddress() + ":" + inetSocketAddress.getPort();
		}

		return "";
	}

	public MessageMongoStoreConfig getMessageMongoStoreConfig() {
		return messageMongoStoreConfig;
	}

	public void setMessageMongoStoreConfig(MessageMongoStoreConfig messageMongoStoreConfig) {
		this.messageMongoStoreConfig = messageMongoStoreConfig;
	}

}
