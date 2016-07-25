package com.alibaba.rocketmq.store.persist.mongo;

public class MessageMongoStoreConfig {

	private String mongoRepSetHosts;

	private String mongoDbName;

	private String mongoUser;

	private String mongoPassword;

	public String getMongoUser() {
		return mongoUser;
	}

	public void setMongoUser(String mongoUser) {
		this.mongoUser = mongoUser;
	}

	public String getMongoPassword() {
		return mongoPassword;
	}

	public void setMongoPassword(String mongoPassword) {
		this.mongoPassword = mongoPassword;
	}

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
