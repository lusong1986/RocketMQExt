package com.alibaba.rocketmq.broker.client;

import java.util.concurrent.ConcurrentHashMap;

public class ConsumerAddressRecorder {

	private static ConcurrentHashMap<String/* channelRemoteAddr */, String/* topic-queueId */> consumerAddressQueueMap = new ConcurrentHashMap<String, String>(
			512);

	public static ConcurrentHashMap<String, String> getConsumerAddressQueueMap() {
		return consumerAddressQueueMap;
	}

	public static void putConsumerAddress(final String addr, final String topicQueue) {
		String queues = consumerAddressQueueMap.get(addr);
		if (null != queues) {
			queues += "," + topicQueue;
		} else {
			queues = topicQueue;
		}
		consumerAddressQueueMap.put(addr, queues);
	}
}
