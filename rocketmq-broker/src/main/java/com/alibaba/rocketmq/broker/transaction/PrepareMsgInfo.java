package com.alibaba.rocketmq.broker.transaction;

public class PrepareMsgInfo {

	private String producerGroup;
	private Long tranStateTableOffset; // queueoffset
	private Long commitLogOffset;
	private Integer commitOrRollback; // MessageSysFlag.TransactionCommitType
	// TransactionRollbackType
	// TransactionNotType

	private Boolean fromTransactionCheck = false;

	private String msgId;

	private String transactionId;

	public String getProducerGroup() {
		return producerGroup;
	}

	public void setProducerGroup(String producerGroup) {
		this.producerGroup = producerGroup;
	}

	public Long getTranStateTableOffset() {
		return tranStateTableOffset;
	}

	public void setTranStateTableOffset(Long tranStateTableOffset) {
		this.tranStateTableOffset = tranStateTableOffset;
	}

	public Long getCommitLogOffset() {
		return commitLogOffset;
	}

	public void setCommitLogOffset(Long commitLogOffset) {
		this.commitLogOffset = commitLogOffset;
	}

	public Integer getCommitOrRollback() {
		return commitOrRollback;
	}

	public void setCommitOrRollback(Integer commitOrRollback) {
		this.commitOrRollback = commitOrRollback;
	}

	public Boolean getFromTransactionCheck() {
		return fromTransactionCheck;
	}

	public void setFromTransactionCheck(Boolean fromTransactionCheck) {
		this.fromTransactionCheck = fromTransactionCheck;
	}

	public String getMsgId() {
		return msgId;
	}

	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

}
