package com.alibaba.rocketmq.broker.offlineconsumer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class OfflineConsumerTest {

	@Test
	public void test_offlineConsumer() throws Exception {

		final String TARGET = "1.1.1.1@2111";
		final List<String> clientIds = new ArrayList<String>();
		clientIds.add(TARGET);
		clientIds.add("2.2.2.2@2111");

		final String filterConsumerClientIds = TARGET + ",3.3.3.3@4567";
		filterOfflineClientIds(clientIds, filterConsumerClientIds);

		System.out.println(clientIds);
		Assert.assertTrue(!clientIds.contains(TARGET));

	}

	@Test
	public void test_offlineConsumer2() throws Exception {

		final String TARGET = "1.1.1.1@2111";
		final List<String> clientIds = new ArrayList<String>();
		clientIds.add(TARGET);
		clientIds.add("2.2.2.2@2111");

		final String filterConsumerClientIds = "1.1.1.1,3.3.3.3@4567";
		filterOfflineClientIds(clientIds, filterConsumerClientIds);

		System.out.println(clientIds);
		Assert.assertTrue(!clientIds.contains(TARGET));

	}
	
	@Test
	public void test_offlineConsumer3() throws Exception {

		final String TARGET = "1.1.1.1@4562";
		final List<String> clientIds = new ArrayList<String>();
		clientIds.add(TARGET);
		clientIds.add("2.2.2.2@2111");

		final String filterConsumerClientIds = "1.1.1.1,3.3.3.3@4567";
		filterOfflineClientIds(clientIds, filterConsumerClientIds);

		System.out.println(clientIds);
		Assert.assertTrue(!clientIds.contains(TARGET));

	}
	

	/**
	 * 根据filterConsumerClientIds过滤clientId
	 * 
	 * @param clientIds
	 * @param filterConsumerClientIds
	 */
	private void filterOfflineClientIds(final List<String> clientIds, final String filterConsumerClientIds) {
		if (StringUtils.isNotBlank(filterConsumerClientIds)) {
			String[] filterConsumerClientIdArray = filterConsumerClientIds.split(",");
			
			
			final Iterator<String> clientIdsIterator = clientIds.iterator();
			while (clientIdsIterator.hasNext()) {
				final String clientId = clientIdsIterator.next();
				final String clientHostIp = clientId.substring(0, clientId.indexOf("@"));
				for (String filterClientId : filterConsumerClientIdArray) {
					if(clientId.equals(filterClientId)  || clientHostIp.equals(filterClientId)){
						clientIdsIterator.remove();
					}
				}
			}
		}
	}
}
