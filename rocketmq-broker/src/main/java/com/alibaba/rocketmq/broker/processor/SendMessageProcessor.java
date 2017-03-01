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
package com.alibaba.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.mqtrace.ConsumeMessageContext;
import com.alibaba.rocketmq.broker.mqtrace.ConsumeMessageHook;
import com.alibaba.rocketmq.broker.mqtrace.SendMessageContext;
import com.alibaba.rocketmq.broker.mqtrace.SendMessageHook;
import com.alibaba.rocketmq.broker.transaction.PrepareMsgInfo;
import com.alibaba.rocketmq.broker.transaction.TransactionPrepareMsgRecoder;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.cat.CatDataConstants;
import com.alibaba.rocketmq.common.cat.CatUtils;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.MessageAccessor;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SendMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SendMessageResponseHeader;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.common.sysflag.TopicSysFlag;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.store.MessageExtBrokerInner;
import com.alibaba.rocketmq.store.PutMessageResult;
import com.alibaba.rocketmq.store.config.StorePathConfigHelper;
import com.dianping.cat.Cat;
import com.dianping.cat.Cat.Context;
import com.dianping.cat.CatConstants;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.spi.MessageTree;

/**
 * 处理客户端发送消息的请求
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class SendMessageProcessor extends AbstractSendMessageProcessor implements NettyRequestProcessor {

	public SendMessageProcessor(final BrokerController brokerController) {
		super(brokerController);
	}

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
			throws RemotingCommandException {
		SendMessageContext mqtraceContext = null;
		RemotingCommand response = null;
		Transaction transaction = null;
		log.info(">>>>>>>>>>>>>>>>>>>>>>>>>SendMessageProcessor.processRequest:" + JSON.toJSONString(request));

		switch (request.getCode()) {
		case RequestCode.CONSUMER_SEND_MSG_BACK:

			transaction = CatUtils.catTransaction(CatDataConstants.CONSUMER_SEND_MSG_BACK,
					CatDataConstants.CONSUMER_SEND_MSG_BACK);
			try {
				response = this.consumerSendMsgBack(ctx, request);
				transaction.addData("request", JSON.toJSONString(request));
				CatUtils.catSuccess(transaction);
			} catch (RemotingCommandException e) {
				CatUtils.catException(transaction, e);
				throw e;
			} finally {
				CatUtils.catComplete(transaction);
			}

			return response;
		default: // RequestCode.SEND_MESSAGE, RequestCode.SEND_MESSAGE_V2

			SendMessageRequestHeader requestHeader = parseRequestHeader(request);
			if (requestHeader == null) {
				return null;
			}
			// 消息轨迹：记录到达 broker 的消息
			mqtraceContext = buildMsgContext(ctx, requestHeader);

			this.executeSendMessageHookBefore(ctx, request, mqtraceContext);

			Cat.buildContextFromMQ(MessageDecoder.string2messageProperties(requestHeader.getProperties()));
			transaction = CatUtils.catTransaction(CatDataConstants.SEND_MESSAGE_V2, CatDataConstants.SEND_MESSAGE_V2);
			try {
				response = this.sendMessage(ctx, request, mqtraceContext, requestHeader);
				log.info(">>>>>>>>>>>>>>>>>>>>>>>>>SendMessageProcessor.processRequest, response:"
						+ JSON.toJSONString(response));

				transaction.addData("request", JSON.toJSONString(request));
				CatUtils.catSuccess(transaction);
			} catch (RemotingCommandException e) {
				CatUtils.catException(transaction, e);
				throw e;
			} finally {
				CatUtils.catComplete(transaction);
			}

			// 消息轨迹：记录发送成功的消息
			this.executeSendMessageHookAfter(response, mqtraceContext);

			return response;
		}
	}

	private RemotingCommand consumerSendMsgBack(final ChannelHandlerContext ctx, final RemotingCommand request)
			throws RemotingCommandException {

		final RemotingCommand response = RemotingCommand.createResponseCommand(null);
		final ConsumerSendMsgBackRequestHeader requestHeader = (ConsumerSendMsgBackRequestHeader) request
				.decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);

		// 消息轨迹：记录消费失败的消息
		if (this.hasConsumeMessageHook() && !UtilAll.isBlank(requestHeader.getOriginMsgId())) {
			// 执行hook
			ConsumeMessageContext context = new ConsumeMessageContext();
			context.setConsumerGroup(requestHeader.getGroup());
			context.setTopic(requestHeader.getOriginTopic());
			context.setClientHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
			context.setSuccess(false);
			context.setStatus(ConsumeConcurrentlyStatus.RECONSUME_LATER.toString());

			Map<String, Long> messageIds = new HashMap<String, Long>();
			messageIds.put(requestHeader.getOriginMsgId(), requestHeader.getOffset());
			context.setMessageIds(messageIds);
			this.executeConsumeMessageHookAfter(context);
		}

		// 确保订阅组存在
		SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager()
				.findSubscriptionGroupConfig(requestHeader.getGroup());
		if (null == subscriptionGroupConfig) {
			response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
			response.setRemark("subscription group not exist, " + requestHeader.getGroup() + " "
					+ FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
			return response;
		}

		// 检查Broker权限
		if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
			response.setCode(ResponseCode.NO_PERMISSION);
			response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
					+ "] sending message is forbidden");
			return response;
		}

		// 如果重试队列数目为0，则直接丢弃消息
		if (subscriptionGroupConfig.getRetryQueueNums() <= 0) {
			response.setCode(ResponseCode.SUCCESS);
			response.setRemark(null);
			return response;
		}

		String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());
		int queueIdInt = Math.abs(this.random.nextInt() % 99999999) % subscriptionGroupConfig.getRetryQueueNums();

		// 如果是单元化模式，则对 topic 进行设置
		int topicSysFlag = 0;
		if (requestHeader.isUnitMode()) {
			topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
		}

		// 检查topic是否存在
		TopicConfig topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(//
				newTopic,//
				subscriptionGroupConfig.getRetryQueueNums(), //
				PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);

		log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>consumerSendMsgBack.topicConfig>>>>>>>>>" + JSON.toJSONString(topicConfig));

		if (null == topicConfig) {
			response.setCode(ResponseCode.SYSTEM_ERROR);
			response.setRemark("topic[" + newTopic + "] not exist");
			return response;
		}

		// 检查topic权限
		if (!PermName.isWriteable(topicConfig.getPerm())) {
			response.setCode(ResponseCode.NO_PERMISSION);
			response.setRemark(String.format("the topic[%s] sending message is forbidden", newTopic));
			return response;
		}

		// 查询消息，这里如果堆积消息过多，会访问磁盘
		// 另外如果频繁调用，是否会引起gc问题，需要关注 TODO
		MessageExt msgExt = this.brokerController.getMessageStore().lookMessageByOffset(requestHeader.getOffset());
		if (null == msgExt) {
			response.setCode(ResponseCode.SYSTEM_ERROR);
			response.setRemark("look message by offset failed, " + requestHeader.getOffset());
			return response;
		}

		// 构造消息
		final String retryTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
		if (null == retryTopic) {
			MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_RETRY_TOPIC, msgExt.getTopic());
		}
		msgExt.setWaitStoreMsgOK(false);

		// 客户端自动决定定时级别
		int delayLevel = requestHeader.getDelayLevel();

		// 死信消息处理
		if (msgExt.getReconsumeTimes() >= subscriptionGroupConfig.getRetryMaxTimes()//
				|| delayLevel < 0) {
			newTopic = MixAll.getDLQTopic(requestHeader.getGroup());

			log.info("#############################>>>>>>>>>>>>>>>>>consumerSendMsgBack DLQ msg:" + newTopic);
			queueIdInt = Math.abs(this.random.nextInt() % 99999999) % DLQ_NUMS_PER_GROUP;

			topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic, //
					DLQ_NUMS_PER_GROUP,//
					PermName.PERM_WRITE, 0 // 死信消息不需要同步，不需要较正。
					);
			if (null == topicConfig) {
				response.setCode(ResponseCode.SYSTEM_ERROR);
				response.setRemark("topic[" + newTopic + "] not exist");
				return response;
			}
		}
		// 继续重试
		else {
			if (0 == delayLevel) {
				delayLevel = 3 + msgExt.getReconsumeTimes();
			}

			msgExt.setDelayTimeLevel(delayLevel);
		}

		MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
		msgInner.setTopic(newTopic);
		msgInner.setBody(msgExt.getBody());
		msgInner.setFlag(msgExt.getFlag());
		MessageAccessor.setProperties(msgInner, msgExt.getProperties());
		msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
		msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));

		msgInner.setQueueId(queueIdInt);
		msgInner.setSysFlag(msgExt.getSysFlag());
		msgInner.setBornTimestamp(msgExt.getBornTimestamp());
		msgInner.setBornHost(msgExt.getBornHost());
		msgInner.setStoreHost(this.getStoreHost());
		msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);

		// 保存源生消息的 msgId
		String originMsgId = MessageAccessor.getOriginMessageId(msgExt);
		MessageAccessor.setOriginMessageId(msgInner, UtilAll.isBlank(originMsgId) ? msgExt.getMsgId() : originMsgId);

		log.info("#############################consume failed msg, store a delay msgInner:" + msgInner);
		PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
		if (putMessageResult != null) {
			switch (putMessageResult.getPutMessageStatus()) {
			case PUT_OK:
				// 统计失败重试的Topic
				String backTopic = msgExt.getTopic();
				String correctTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
				if (correctTopic != null) {
					backTopic = correctTopic;
				}

				this.brokerController.getBrokerStatsManager().incSendBackNums(requestHeader.getGroup(), backTopic);

				response.setCode(ResponseCode.SUCCESS);
				response.setRemark(null);

				return response;
			default:
				break;
			}

			response.setCode(ResponseCode.SYSTEM_ERROR);
			response.setRemark(putMessageResult.getPutMessageStatus().name());
			return response;
		}

		response.setCode(ResponseCode.SYSTEM_ERROR);
		response.setRemark("putMessageResult is null");
		return response;
	}

	private String diskUtil() {
		String storePathPhysic = this.brokerController.getMessageStoreConfig().getStorePathCommitLog();
		double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);

		String storePathLogis = StorePathConfigHelper.getStorePathConsumeQueue(this.brokerController
				.getMessageStoreConfig().getStorePathRootDir());
		double logisRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogis);

		String storePathIndex = StorePathConfigHelper.getStorePathIndex(this.brokerController.getMessageStoreConfig()
				.getStorePathRootDir());
		double indexRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathIndex);

		return String.format("CL: %5.2f CQ: %5.2f INDEX: %5.2f", physicRatio, logisRatio, indexRatio);
	}

	public static Map<String, String> buildMQUserProperties() {
		try {
			MessageTree tree = Cat.getManager().getThreadLocalMessageTree();
			String messageId = tree.getMessageId();

			if (messageId == null) {
				messageId = Cat.createMessageId();
				tree.setMessageId(messageId);
			}

			String childId1 = Cat.createMessageId();
			Cat.logEvent(CatConstants.TYPE_REMOTE_CALL, "", Event.SUCCESS, childId1);

			String root = tree.getRootMessageId();

			if (root == null) {
				root = messageId;
			}

			Map<String, String> map = new HashMap<String, String>();
			map.put(Context.ROOT, root);
			map.put(Context.PARENT + 1, messageId);
			map.put(Context.CHILD + 1, childId1);

			return map;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private RemotingCommand sendMessage(final ChannelHandlerContext ctx, //
			final RemotingCommand request,//
			final SendMessageContext mqtraceContext,//
			final SendMessageRequestHeader requestHeader) throws RemotingCommandException {
		log.info(">>>>>>>>>>>>sendMessage>>>requestHeader:" + JSON.toJSONString(requestHeader));

		final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
		final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();

		// 由于有直接返回的逻辑，所以必须要设置
		response.setOpaque(request.getOpaque());

		if (log.isDebugEnabled()) {
			// log.debug("receive SendMessage request command, " + request);
		}
		response.setCode(-1);
		super.msgCheck(ctx, requestHeader, response);
		if (response.getCode() != -1) {
			return response;
		}

		final byte[] body = request.getBody();

		int queueIdInt = requestHeader.getQueueId();
		log.info(">>>>>>>>>>>>sendMessage>>> requestHeader.getQueueId():" + requestHeader.getQueueId());
		TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(
				requestHeader.getTopic());
		// 随机指定一个队列
		if (queueIdInt < 0) {
			queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
		}
		log.info(">>>>>>>>>>>>sendMessage>>>queueIdInt:" + queueIdInt);

		int sysFlag = requestHeader.getSysFlag();
		// 多标签过滤需要置位
		if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
			sysFlag |= MessageSysFlag.MultiTagsFlag;
		}

		MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
		msgInner.setTopic(requestHeader.getTopic());
		msgInner.setBody(body);
		msgInner.setFlag(requestHeader.getFlag());

		Map<String, String> string2messageProperties = MessageDecoder.string2messageProperties(requestHeader
				.getProperties());
		string2messageProperties.putAll(buildMQUserProperties());
		MessageAccessor.setProperties(msgInner, string2messageProperties);
		msgInner.setPropertiesString(MessageDecoder.messageProperties2String(string2messageProperties));
		log.info(">>>>>>>>>>>>string2messageProperties:" + string2messageProperties);

		msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(topicConfig.getTopicFilterType(),
				msgInner.getTags()));

		msgInner.setQueueId(queueIdInt);
		msgInner.setSysFlag(sysFlag);
		msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
		msgInner.setBornHost(ctx.channel().remoteAddress());
		msgInner.setStoreHost(this.getStoreHost());
		msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());

		// 检查事务消息
		if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
			String traFlag = msgInner.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
			if (traFlag != null) {
				response.setCode(ResponseCode.NO_PERMISSION);
				response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
						+ "] sending transaction message is forbidden");
				return response;
			}
		}

		PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);

		log.info(">>>>>>>>>>>>>>>>>>>>>>>>>SendMessageProcessor.sendMessage, putMessageResult:"
				+ JSON.toJSONString(putMessageResult));
		if (putMessageResult != null) {
			boolean sendOK = false;

			switch (putMessageResult.getPutMessageStatus()) {
			// Success
			case PUT_OK:
				sendOK = true;
				response.setCode(ResponseCode.SUCCESS);
				break;
			case FLUSH_DISK_TIMEOUT:
				response.setCode(ResponseCode.FLUSH_DISK_TIMEOUT);
				sendOK = true;
				break;
			case FLUSH_SLAVE_TIMEOUT:
				response.setCode(ResponseCode.FLUSH_SLAVE_TIMEOUT);
				sendOK = true;
				break;
			case SLAVE_NOT_AVAILABLE:
				response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
				sendOK = true;
				break;

			// Failed
			case CREATE_MAPEDFILE_FAILED:
				response.setCode(ResponseCode.SYSTEM_ERROR);
				response.setRemark("create maped file failed, please make sure OS and JDK both 64bit.");
				break;
			case MESSAGE_ILLEGAL:
				response.setCode(ResponseCode.MESSAGE_ILLEGAL);
				response.setRemark("the message is illegal, maybe length not matched.");
				break;
			case SERVICE_NOT_AVAILABLE:
				response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
				response.setRemark("service not available now, maybe disk full, " + diskUtil()
						+ ", maybe your broker machine memory too small.");
				break;
			case UNKNOWN_ERROR:
				response.setCode(ResponseCode.SYSTEM_ERROR);
				response.setRemark("UNKNOWN_ERROR");
				break;
			default:
				response.setCode(ResponseCode.SYSTEM_ERROR);
				response.setRemark("UNKNOWN_ERROR DEFAULT");
				break;
			}

			if (sendOK) {
				// 统计
				this.brokerController.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
				this.brokerController.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(),
						putMessageResult.getAppendMessageResult().getWroteBytes());
				this.brokerController.getBrokerStatsManager().incBrokerPutNums();

				response.setRemark(null);

				putPrepareMsg(msgInner, putMessageResult);

				responseHeader.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
				responseHeader.setQueueId(queueIdInt);
				responseHeader.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());

				// 直接返回
				doResponse(ctx, request, response);
				if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
					// 如果消费者是长轮询，立即通知消息到达
					log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>notifyMessageArriving >>>queueIdInt:" + queueIdInt
							+ ",requestHeader.getTopic():" + requestHeader.getTopic());
					this.brokerController.getPullRequestHoldService().notifyMessageArriving(requestHeader.getTopic(),
							queueIdInt, putMessageResult.getAppendMessageResult().getLogicsOffset() + 1);
				}

				// 消息轨迹：记录发送成功的消息
				if (hasSendMessageHook()) {
					mqtraceContext.setMsgId(responseHeader.getMsgId());
					mqtraceContext.setQueueId(responseHeader.getQueueId());
					mqtraceContext.setQueueOffset(responseHeader.getQueueOffset());
				}
				return null;
			}
		} else {
			response.setCode(ResponseCode.SYSTEM_ERROR);
			response.setRemark("store putMessage return null");
		}

		return response;
	}

	private void putPrepareMsg(MessageExtBrokerInner msgInner, PutMessageResult putMessageResult) {
		try {
			String tranFlag = msgInner.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
			if (tranFlag != null) {
				final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
				if (tranType == MessageSysFlag.TransactionPreparedType && msgInner.getPreparedTransactionOffset() == 0L) {
					PrepareMsgInfo prepareMsg = new PrepareMsgInfo();
					prepareMsg.setCommitLogOffset(putMessageResult.getAppendMessageResult().getWroteOffset());
					prepareMsg.setCommitOrRollback(MessageSysFlag.TransactionNotType);
					prepareMsg.setFromTransactionCheck(false);
					prepareMsg.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());

					final String pgGroup = msgInner.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
					if (pgGroup != null) {
						prepareMsg.setProducerGroup(pgGroup);
					}
					// prepareMsg.setTransactionId(transactionId); //TODO
					prepareMsg.setTranStateTableOffset(putMessageResult.getAppendMessageResult().getLogicsOffset()); // queueoffset
					log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>SendMessageProcessor.sendMessage>>>prepareMsg:"
							+ JSON.toJSONString(prepareMsg));
					TransactionPrepareMsgRecoder.putPrepareMsg(prepareMsg);
				}
			}
		} catch (Throwable e) {
			log.warn("putPrepareMsg error :" + e.getMessage(), e);
		}
	}

	public SocketAddress getStoreHost() {
		return storeHost;
	}

	/**
	 * 发送每条消息会回调
	 */
	private List<SendMessageHook> sendMessageHookList;

	public boolean hasSendMessageHook() {
		return sendMessageHookList != null && !this.sendMessageHookList.isEmpty();
	}

	public void registerSendMessageHook(List<SendMessageHook> sendMessageHookList) {
		this.sendMessageHookList = sendMessageHookList;
	}

	/**
	 * 消费每条消息会回调
	 */
	private List<ConsumeMessageHook> consumeMessageHookList;

	public boolean hasConsumeMessageHook() {
		return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
	}

	public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
		this.consumeMessageHookList = consumeMessageHookList;
	}

	public void executeConsumeMessageHookAfter(final ConsumeMessageContext context) {
		if (hasConsumeMessageHook()) {
			for (ConsumeMessageHook hook : this.consumeMessageHookList) {
				try {
					hook.consumeMessageAfter(context);
				} catch (Throwable e) {
				}
			}
		}
	}
}
