package com.alibaba.rocketmq.store.persist.mongo;

public class MessageMongoStoreConfig {

	private String mongoRepSetHosts;

	private String mongoDbName;

	public String getMongoDbName() {
		return mongoDbName;
	}

	public void setMongoDbName(String mongoDbName) {
		this.mongoDbName = mongoDbName;
	}

	public String getMongoRepSetHosts() {
		return mongoRepSetHosts;
	}

	public void setMongoRepSetHosts(String mongoRepSetHosts) {
		this.mongoRepSetHosts = mongoRepSetHosts;
	}

}
