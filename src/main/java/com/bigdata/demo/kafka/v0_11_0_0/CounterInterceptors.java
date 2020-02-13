package com.bigdata.demo.kafka.v0_11_0_0;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

class CounterInterceptor implements ProducerInterceptor<String, String> {
	private int errorCounter = 0;
	private int successCounter = 0;

	@Override
	public void configure( Map<String, ?> configs) { // 配置
	}

	@Override
	public ProducerRecord<String, String> onSend( ProducerRecord<String, String> record) {
		return record;
	}

	@Override
	public void onAcknowledgement( RecordMetadata metadata, Exception exception) {
		// 统计成功和失败的次数
		if (exception == null) {
			successCounter++;
		} else {
			errorCounter++;
			System.out.println ( exception.getCause ());
		}
	}

	@Override
	public void close() {
		// 保存结果
		System.out.println("Successful sent: " + successCounter);
		System.out.println("Failed sent: " + errorCounter);
	}
}

class TimeInterceptor implements ProducerInterceptor<String, String> {

	@Override
	public void configure( Map<String, ?> configs) {

	}

	@Override
	public ProducerRecord<String, String> onSend( ProducerRecord<String, String> record) {
		// 对已经创建消息进行重新封装
		return new ProducerRecord(
				record.topic(),
				record.partition(),
				record.timestamp(),
				record.key(),
				String.format ( "%s,%s",System.currentTimeMillis(), record.value().toString ())
		);
	}

	@Override
	public void onAcknowledgement( RecordMetadata metadata, Exception exception) {
		// 消息消费完毕，或是发送异常，执行
	}

	@Override
	public void close() {
		// 清理
	}
}