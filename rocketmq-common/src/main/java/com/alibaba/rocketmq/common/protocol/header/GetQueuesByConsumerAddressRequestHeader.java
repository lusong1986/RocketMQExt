package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;

/**
 * 
 * 
 * @author lusong
 *
 */
public class GetQueuesByConsumerAddressRequestHeader implements CommandCustomHeader {
	@CFNotNull
	private String consumerAddress;

	public String getConsumerAddress() {
		return consumerAddress;
	}

	public void setConsumerAddress(String consumerAddress) {
		this.consumerAddress = consumerAddress;
	}

	@Override
	public void checkFields() throws RemotingCommandException {
	}

}
