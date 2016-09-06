package com.alibaba.rocketmq.broker.client;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerAddressRecorder {

	private static ConcurrentHashMap<String/* channelRemoteAddr */, Set<String>/* topic-queueId */> consumerAddressQueueMap = new ConcurrentHashMap<String, Set<String>>(
			512);

	public static ConcurrentHashMap<String, Set<String>> getConsumerAddressQueueMap() {
		return consumerAddressQueueMap;
	}

	public static void putConsumerAddress(final String addr, final String topicQueue) {
		Set<String> queueSet = new HashSet<String>();
		Set<String> old = consumerAddressQueueMap.putIfAbsent(addr, queueSet);
		if (null != old) {
			queueSet = old;
		}

		queueSet.add(topicQueue);
	}
}
