/**
 * $Id: MapedFileTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

import static org.junit.Assert.assertTrue;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;

public class MapedFileTest {

	private static final String StoreMessage = "Once, there was a chance for me!";

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void comapre() throws InterruptedException, IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(
				"C:\\Users\\Lusong\\Desktop\\rmq_ms_notsame\\slave\\slave.txt"))));

		HashMap<String, String> slaves = new HashMap<String, String>(1000000);
		while (true) {
			String msgId = reader.readLine();
			if (StringUtils.isNotBlank(msgId)) {
				slaves.put(msgId, "");
			} else {
				break;
			}
		}
		System.out.println(slaves.size());

		BufferedReader reader2 = new BufferedReader(new InputStreamReader(new FileInputStream(new File(
				"C:\\Users\\Lusong\\Desktop\\rmq_ms_notsame\\master\\master.txt"))));

		while (true) {
			String msgId = reader2.readLine();
			if (StringUtils.isNotBlank(msgId) && !slaves.containsKey(msgId)) {
				System.out.println(">>>>>>>>>>>>not contain msgId:" +msgId);
				break;
			}
		}
		System.out.println("aaa");

	}

	@Test
	public void decodeAllMsgsFromFile() throws InterruptedException {
		try {
			int mapedFileSizeCommitLog = 1024 * 1024 * 1024;
			MapedFile mapedFile = new MapedFile("C:\\Users\\Lusong\\Downloads\\slave\\00000000024696061952",
					mapedFileSizeCommitLog);
			System.out.println(ToStringBuilder.reflectionToString(mapedFile));
			mapedFile.setWrotePostion(mapedFileSizeCommitLog);

			final long fileFromOffset = mapedFile.getFileFromOffset();

			BufferedOutputStream fileOutputStream = new BufferedOutputStream(new FileOutputStream(new File(
					"C:\\Users\\Lusong\\Desktop\\rmq_ms_notsame\\slave\\slave.txt")));

			int counter = 0;
			int pos = (int) (fileFromOffset % mapedFileSizeCommitLog);
			System.out.println(">>>>>>>pos:" + pos + ">>>>>>>>>>fileFromOffset:" + fileFromOffset);
			while (true) {
				int msgSize = 0;
				try {
					final SelectMapedBufferResult sizeResult = mapedFile.selectMapedBuffer(pos, 4);
					msgSize = sizeResult.getByteBuffer().getInt();
					System.out.println(">>>>>>>>>>>>>>msgSize:" + msgSize);

					final SelectMapedBufferResult sbr = mapedFile.selectMapedBuffer(pos, msgSize);
					MessageExt messageExt = MessageDecoder.decode(sbr.getByteBuffer(), true, false);
					// System.out.println(">>>>>>>>>>>>>>messageExt:" + messageExt);

					fileOutputStream.write(messageExt.getMsgId().getBytes());
					fileOutputStream.write("\n".getBytes());
					fileOutputStream.flush();

				} catch (Exception e) {
					e.printStackTrace();
					break;
				} finally {
					System.out.println(counter);
				}

				pos += msgSize;
				counter++;
			}
			System.out.println(counter);
			fileOutputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test_write_read() {
		try {
			MapedFile mapedFile = new MapedFile("./unit_test_store/MapedFileTest/000", 1024 * 64);
			boolean result = mapedFile.appendMessage(StoreMessage.getBytes());
			assertTrue(result);
			System.out.println("write OK");

			SelectMapedBufferResult selectMapedBufferResult = mapedFile.selectMapedBuffer(0);
			byte[] data = new byte[StoreMessage.length()];
			selectMapedBufferResult.getByteBuffer().get(data);
			String readString = new String(data);

			System.out.println("Read: " + readString);
			assertTrue(readString.equals(StoreMessage));

			// 禁止Buffer读写
			mapedFile.shutdown(1000);

			// mapedFile对象不可用
			assertTrue(!mapedFile.isAvailable());

			// 释放读到的Buffer
			selectMapedBufferResult.release();

			// 内存真正释放掉
			assertTrue(mapedFile.isCleanupOver());

			// 文件删除成功
			assertTrue(mapedFile.destroy(1000));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 当前测试用例由于对mmap操作错误，会导致JVM CRASHED
	 */
	@Ignore
	public void test_jvm_crashed() {
		try {
			MapedFile mapedFile = new MapedFile("./unit_test_store/MapedFileTest/10086", 1024 * 64);
			boolean result = mapedFile.appendMessage(StoreMessage.getBytes());
			assertTrue(result);
			System.out.println("write OK");

			SelectMapedBufferResult selectMapedBufferResult = mapedFile.selectMapedBuffer(0);
			selectMapedBufferResult.release();
			mapedFile.shutdown(1000);

			byte[] data = new byte[StoreMessage.length()];
			selectMapedBufferResult.getByteBuffer().get(data);
			String readString = new String(data);
			System.out.println(readString);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
