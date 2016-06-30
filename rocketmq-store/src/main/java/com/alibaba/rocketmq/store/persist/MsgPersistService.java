/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.alibaba.rocketmq.store.persist;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import com.alibaba.rocketmq.store.persist.mongo.MessageMongoStore;
import com.alibaba.rocketmq.store.persist.mongo.MessageMongoStoreConfig;
import com.alibaba.rocketmq.store.persist.mysql.MessageDBStore;
import com.alibaba.rocketmq.store.persist.mysql.MessageDBStoreConfig;

/**
 * 消息持久化服务
 * 
 * @author lusong
 * @since 2016-06-01
 */
public class MsgPersistService extends ServiceThread {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

	private static final String MYSQL = "mysql";

	private static final String MONGO = "mongo";

	private MsgStore msgStore;

	private volatile boolean opened = false;

	private LinkedBlockingQueue<MessageExt> msgQueue = new LinkedBlockingQueue<MessageExt>(100000);

	public MsgPersistService(final DefaultMessageStore store) {
		log.info("Start to init msgStore in MsgPersistService Constrcutor.");
		MessageStoreConfig messageStoreConfig = store.getMessageStoreConfig();
		if (messageStoreConfig != null) {
			if (MONGO.equals(messageStoreConfig.getMsgStoreType())) {
				MessageMongoStoreConfig messageMongoStoreConfig = new MessageMongoStoreConfig();
				messageMongoStoreConfig.setMongoDbName(messageStoreConfig.getMongoDbName());
				messageMongoStoreConfig.setMongoRepSetHosts(messageStoreConfig.getMongoRepSetHosts());

				this.msgStore = new MessageMongoStore(messageMongoStoreConfig);
			} else if (MYSQL.equals(messageStoreConfig.getMsgStoreType())) {
				MessageDBStoreConfig messageDBStoreConfig = new MessageDBStoreConfig();
				messageDBStoreConfig.setJdbcURL(messageStoreConfig.getJdbcURL());
				messageDBStoreConfig.setJdbcUser(messageStoreConfig.getJdbcUser());
				messageDBStoreConfig.setJdbcPassword(messageStoreConfig.getJdbcPassword());

				this.msgStore = new MessageDBStore(messageDBStoreConfig);
			}

			if (msgStore != null) {
				opened = msgStore.open();
			}
		}

		if (opened) {
			log.info(">>>>>>>>>>>get msgStore connection correctly.");
		} else {
			log.warn(">>>>>>>>>>>get msgStore connection wrong.");
		}
	}

	/**
	 * 向队列中添加message，队列满情况下，丢弃请求
	 */
	public void putMessage(final MessageExt msg) {
		if (!opened) {
			return;
		}

		boolean offer = this.msgQueue.offer(msg);
		if (!offer) {
			if (log.isDebugEnabled()) {
				log.debug("putMessage msg failed, {}", msg);
			}
		}
	}

	@Override
	public void run() {
		log.info(this.getServiceName() + " service started");

		while (!this.isStoped()) {
			try {
				MessageExt msg = this.msgQueue.poll(3000, TimeUnit.MILLISECONDS);
				if (msg != null) {
					log.info("Start to persist msg in MsgPersistService:" + msg);

					this.persist(msg);
				}
			} catch (Exception e) {
				log.warn(this.getServiceName() + " service has exception. ", e);
			}
		}

		log.info(this.getServiceName() + " service end");
	}

	private boolean persist(MessageExt msg) {
		List<MessageExt> msgs = new ArrayList<MessageExt>();
		msgs.add(msg);
		msgStore.store(msgs);

		return false;
	}

	@Override
	public String getServiceName() {
		return MsgPersistService.class.getSimpleName();
	}
}
