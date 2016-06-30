package com.alibaba.rocketmq.store.persist;

import java.util.List;

import com.alibaba.rocketmq.common.message.MessageExt;

public interface MsgStore {

	public boolean open();

	public void close();

	public boolean store(final List<MessageExt> msgs);

}
