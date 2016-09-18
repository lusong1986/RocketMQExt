package com.alibaba.rocketmq.broker.client;

import java.util.concurrent.ConcurrentHashMap;

public class ConsumerAddressRecorder {

	private static ConcurrentHashMap<String/* channelRemoteAddr */, ConcurrentHashMap<String, String>/* topic-queueId */> consumerAddressQueueMap = new ConcurrentHashMap<String, ConcurrentHashMap<String, String>>(
			512);

	public static ConcurrentHashMap<String, ConcurrentHashMap<String, String>> getConsumerAddressQueueMap() {
		return consumerAddressQueueMap;
	}

	public static void putConsumerAddress(final String addr, final String topicQueue) {
		ConcurrentHashMap<String, String> queueSet = new ConcurrentHashMap<String, String>();
		ConcurrentHashMap<String, String> old = consumerAddressQueueMap.putIfAbsent(addr, queueSet);
		if (null != old) {
			queueSet = old;
		}

		queueSet.put(topicQueue, "1");
	}
}
