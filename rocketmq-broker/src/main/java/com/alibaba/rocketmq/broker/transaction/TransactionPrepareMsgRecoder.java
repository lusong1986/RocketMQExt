package com.alibaba.rocketmq.broker.transaction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionPrepareMsgRecoder {

	// 内存中的最大未回查的预发消息数
	private static final int MAX_UNCONFIRM_PREPARE_MSG_SIZE = 70000;

	// 预发消息最多回查十次仍然不知道是回滚还是提交，就不再回查
	public static final int MAX_CHECK_PREPARE_MSG_TIMES = 10;

	private static final int MAX_PREPARE_MSG_COUNT = 100000;

	// key是预发消息的commitlog offset，value是预发消息
	private static ConcurrentHashMap<Long, PrepareMsgInfo> offsetPrepareMsgMap = new ConcurrentHashMap<Long, PrepareMsgInfo>(
			MAX_PREPARE_MSG_COUNT);

	// 回查预发消息的次数，超过一定次数就不再回查
	private final static ConcurrentHashMap<Long, AtomicInteger> checkPrepareTimes = new ConcurrentHashMap<Long, AtomicInteger>(
			20000);

	public static AtomicInteger getCheckTimes(Long commitLogOffset) {
		AtomicInteger times = checkPrepareTimes.get(commitLogOffset);
		if (null == times) {
			times = new AtomicInteger(0);
			checkPrepareTimes.put(commitLogOffset, times);
		}
		return times;
	}

	public static void removePrepareMsgRecord(Long commitLogOffset) {
		checkPrepareTimes.remove(commitLogOffset);
		offsetPrepareMsgMap.remove(commitLogOffset);
	}

	public static ConcurrentHashMap<Long, PrepareMsgInfo> getPrepareMsgmap() {
		return offsetPrepareMsgMap;
	}

	public static void putPrepareMsg(PrepareMsgInfo prepareMsg) {
		if (offsetPrepareMsgMap.size() >= MAX_UNCONFIRM_PREPARE_MSG_SIZE) {
			// 防止内存溢出，不再处理预发消息
			return;
		}

		offsetPrepareMsgMap.put(prepareMsg.getCommitLogOffset(), prepareMsg);
	}

}
