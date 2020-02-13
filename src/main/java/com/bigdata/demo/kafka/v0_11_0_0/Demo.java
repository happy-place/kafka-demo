package com.bigdata.demo.kafka.v0_11_0_0;

import com.alibaba.fastjson.JSONPObject;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import java.util.*;

public class Demo {

    private static void autoCommitConsume ( String topic){
        /**
         * 连接 broker,从最新开始消费，自动提交 offset 到 kafka 集群
         * 优点：offset 管理自动化
         * 缺点：broker 宕机，存储在上面的 offset 信息会丢失，消息连续消费存在问题
         */
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        // 制定consumer group
        props.put("group.id", UUID.randomUUID().toString());
        // 是否自动确认offset
        props.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // 从最早开始消费
        //        props.put("auto.offset.reset", "earliest");
        props.put("auto.offset.reset", "latest");
        // key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        // value的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 定义consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 消费者订阅的topic, 可同时订阅多个
        //        consumer.subscribe( Arrays.asList("first", "second","third"));
        consumer.subscribe( Arrays.asList(topic));

        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("tmstp=%d partition=%d, offset = %d, key = %s, value = %s%n",
                        record.timestamp (),record.partition (), record.offset(), record.key(), record.value());
        }

    }

    private static void selfCommitConsume ( String topic){
        /**
         * 连接 broker,从最新开始消费，自动提交 offset 到 kafka 集群
         * 优点：offset 管理自动化
         * 缺点：broker 宕机，存储在上面的 offset 信息会丢失，消息连续消费存在问题
         */
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        // 制定consumer group
        props.put("group.id", UUID.randomUUID().toString());
        // 是否自动确认offset
        props.put("enable.auto.commit", "false");
        // 自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // 从最早开始消费
        //        props.put("auto.offset.reset", "earliest");
        props.put("auto.offset.reset", "latest");
        // key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        // value的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 定义consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 消费者订阅的topic, 可同时订阅多个
        //        consumer.subscribe( Arrays.asList("first", "second","third"));
        consumer.subscribe( Arrays.asList(topic));

        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("tmstp=%d partition=%d, offset = %d, key = %s, value = %s%n",
                        record.timestamp (),record.partition (), record.offset(), record.key(), record.value());
            consumer.commitSync (); // 同步提交
//            consumer.commitAsync (); //  异步提交
        }

    }


    private static void noKeyRecord(String topic){
        /**
         * ProducerRecord 定义时，不填 key 表示 key 为 null, 此时只需设置 props.put("serializer.class", "kafka.serializer.StringEncoder");
         * 只针对 value 进行序列化。消息存储的分区是从当前可用分区随机查找一个确定的
         */
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        props.put("request.required.acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Integer, String> producer = null;
        try {
            producer = new KafkaProducer<> ( props );
            for(int i=0;i<20;i++){
                // 不设置 key和 partition
                ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, i+" helllo");
                producer.send(record);
            }
        } catch (Exception e) {
            e.printStackTrace ( );
        } finally {
            if(null!=producer){
                producer.close ();
            }
        }
    }

    private static void keyedRecord(String topic){
        /**
         * ProducerRecord 定义时，不填 key 表示 key 为 null, 此时只需设置 props.put("serializer.class", "kafka.serializer.StringEncoder");
         * 只针对 value 进行序列化。消息存储的分区是从当前可用分区随机查找一个确定的
         */
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        props.put("request.required.acks", "1"); // ISR 副本有一个同步完毕就向 leader 响应，然后 leader 向 client 响应
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Integer, String> producer = null;
        try {
            producer = new KafkaProducer<> ( props );
            for(int i=0;i<50;i++){
                // 设置 key ,但未设置分区，随机选择分区
                ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, i,i+" helllo".toUpperCase ());
                producer.send(record);
            }
        } catch (Exception e) {
            e.printStackTrace ( );
        } finally {
            if(null!=producer){
                producer.close ();
            }
        }
    }

    private static void partitionedRecord(String topic){
        /**
         * ProducerRecord 定义时，不填 key 表示 key 为 null, 此时只需设置 props.put("serializer.class", "kafka.serializer.StringEncoder");
         * 只针对 value 进行序列化。消息存储的分区是从当前可用分区随机查找一个确定的
         */
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        props.put("request.required.acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Integer, String> producer = null;
        try {
            producer = new KafkaProducer<> ( props );
            for(int i=0;i<20;i++){
                // 往指定 分区插入
                ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, 0,i,i+" helllo".toUpperCase ());
                producer.send(record);
            }
        } catch (Exception e) {
            e.printStackTrace ( );
        } finally {
            if(null!=producer){
                producer.close ();
            }
        }
    }

    private static void tmstpRecord(String topic){
        /**
         * ProducerRecord 定义时，不填 key 表示 key 为 null, 此时只需设置 props.put("serializer.class", "kafka.serializer.StringEncoder");
         * 只针对 value 进行序列化。消息存储的分区是从当前可用分区随机查找一个确定的
         */
        Properties props = new Properties();
        // brokers 列表
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        // 全部 ISR 节点同步完毕，再逐级向 leader ,client 返回响应
        props.put("acks", "all");
        // 消息发送最大尝试次数 （broken 反馈超时，重新尝试发送）
        props.put("retries", 0);
        // 消息批处理
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Integer, String> producer = null;
        try {
            producer = new KafkaProducer<> ( props );
            for(int key=0;key<20;key++){
                // 指定了分区，毫秒数(到达时间)，key value
                int partition = new Random ( ).nextInt ( 3 ); // 随机整数分区
                long tmstp = System.currentTimeMillis ( ); // 消息时间戳
                String value = key + " helllo".toUpperCase ( );
                ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, partition,tmstp,key,value);
                producer.send(record);
                Thread.sleep ( 100 );
            }
        } catch (Exception e) {
            e.printStackTrace ( );
        } finally {
            if(null!=producer){
                producer.close ();
            }
        }
    }

    private static void callbackRecord(String topic){
        /**
         * ProducerRecord 定义时，不填 key 表示 key 为 null, 此时只需设置 props.put("serializer.class", "kafka.serializer.StringEncoder");
         * 只针对 value 进行序列化。消息存储的分区是从当前可用分区随机查找一个确定的
         */
        Properties props = new Properties();
        // brokers 列表
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        // 全部 ISR 节点同步完毕，再逐级向 leader ,client 返回响应
        props.put("acks", "all");
        // 消息发送最大尝试次数 （broken 反馈超时，重新尝试发送）
        props.put("retries", 0);
        // 消息批处理
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Integer, String> producer = null;
        try {
            producer = new KafkaProducer<> ( props );
            for(int key=0;key<20;key++){
                // 指定了分区，毫秒数(到达时间)，key value
                int partition = new Random ( ).nextInt ( 3 ); // 随机整数分区
                long tmstp = System.currentTimeMillis ( ); // 消息时间戳
                String value = key + " callback".toUpperCase ( );
                ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, partition,tmstp,key,value);
                // 注册回调，消息成功发送，收到 leader ack 响应后，执行回调
                producer.send(record,new Callback (){
                    @Override
                    public void onCompletion ( RecordMetadata recordMetadata , Exception e ) {
                        int partition = recordMetadata.partition ( );
                        long offset = recordMetadata.offset ( );
                        int keySize = recordMetadata.serializedKeySize ( );
                        int valueSize = recordMetadata.serializedValueSize ( );
                        long tmstp = recordMetadata.timestamp ( );
                        String topic1 = recordMetadata.topic ( );
                        long checksum = recordMetadata.checksum ( );
                        System.out.println ( String.format (
                                "checksum: %s ,topic: %s, tmstp: %s, partition: %s, offset: %s, keySize: %s, valueSize: %s",
                                checksum, topic1, tmstp, partition, offset,keySize , valueSize
                        ) );
                    }
                });
                Thread.sleep ( 100 );
            }
        } catch (Exception e) {
            e.printStackTrace ( );
        } finally {
            if(null!=producer){
                producer.close ();
            }
        }
    }


    private static void customerPartition(String topic){
        /**
         * 自定义分区
         *
         */
        Properties props = new Properties();
        // brokers 列表
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        // 全部 ISR 节点同步完毕，再逐级向 leader ,client 返回响应
        props.put("acks", "all");
        // 消息发送最大尝试次数 （broken 反馈超时，重新尝试发送）
        props.put("retries", 0);
        // 消息批处理
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("partitioner.class", "com.bigdata.demo.kafka.v0_11_0_0.CustomerPartitioner");

        Producer<Integer, String> producer = null;
        try {
            producer = new KafkaProducer<> ( props );
            for(int key=0;key<20;key++){
                String value = key + " customer".toUpperCase ( );
                ProducerRecord<Integer, String> record = new ProducerRecord<>(topic,key,value);
                // 回调
                Callback callback = new Callback() {
                    @Override
                    public void onCompletion ( RecordMetadata recordMetadata , Exception e ) {
                        System.out.println ( String.format (
                                "checksum: %s ,topic: %s, tmstp: %s, partition: %s, offset: %s, keySize: %s, valueSize: %s" ,
                                recordMetadata.checksum ( ) ,
                                recordMetadata.topic ( ) ,
                                recordMetadata.timestamp ( ) ,
                                recordMetadata.partition ( ) ,
                                recordMetadata.offset ( ) ,
                                recordMetadata.serializedKeySize ( ) ,
                                recordMetadata.serializedValueSize ( )
                        ) );
                    }
                };
                // 注册回调，消息成功发送，收到 leader ack 响应后，执行回调
                producer.send(record,callback);
            }
        } catch (Exception e) {
            e.printStackTrace ( );
        } finally {
            if(null!=producer){
                producer.close ();
            }
        }
    }

    private static void interceptorChain(String topic){
        /**
         * 自定义分区
         *
         */
        Properties props = new Properties();
        // brokers 列表
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        // 全部 ISR 节点同步完毕，再逐级向 leader ,client 返回响应
        props.put("acks", "all");
        // 消息发送最大尝试次数 （broken 反馈超时，重新尝试发送）
        props.put("retries", 0);
        // 消息批处理
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        // 2 构建拦截链 (按顺序调用)
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.bigdata.demo.kafka.v0_11_0_0.TimeInterceptor");
        interceptors.add("com.bigdata.demo.kafka.v0_11_0_0.CounterInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);


        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<> ( props );
            for(int key=0;key<20;key++){
                String value = key + " interceptor".toUpperCase ( );
                ProducerRecord<String, String> record = new ProducerRecord<>(topic,key+"",value);
                // 回调
                Callback callback = new Callback() {
                    @Override
                    public void onCompletion ( RecordMetadata recordMetadata , Exception e ) {
                        System.out.println ( String.format (
                                "checksum: %s ,topic: %s, tmstp: %s, partition: %s, offset: %s, keySize: %s, valueSize: %s" ,
                                recordMetadata.checksum ( ) ,
                                recordMetadata.topic ( ) ,
                                recordMetadata.timestamp ( ) ,
                                recordMetadata.partition ( ) ,
                                recordMetadata.offset ( ) ,
                                recordMetadata.serializedKeySize ( ) ,
                                recordMetadata.serializedValueSize ( )
                        ) );
                    }
                };
                // 注册回调，消息成功发送，收到 leader ack 响应后，执行回调
                producer.send(record,callback);
            }
        } catch (Exception e) {
            e.printStackTrace ( );
        } finally {
            if(null!=producer){
                producer.close ();
            }
        }
    }

    private static void jsonRecord(String topic){
        /**
         * ProducerRecord 定义时，不填 key 表示 key 为 null, 此时只需设置 props.put("serializer.class", "kafka.serializer.StringEncoder");
         * 只针对 value 进行序列化。消息存储的分区是从当前可用分区随机查找一个确定的
         */
        Properties props = new Properties();
        // brokers 列表
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        // 全部 ISR 节点同步完毕，再逐级向 leader ,client 返回响应
        props.put("acks", "all");
        // 消息发送最大尝试次数 （broken 反馈超时，重新尝试发送）
        props.put("retries", 0);
        // 消息批处理
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Integer, String> producer = null;
        try {
            producer = new KafkaProducer<> ( props );
            for(int key=0;key<20;key++){
                // 指定了分区，毫秒数(到达时间)，key value
                int partition = new Random ( ).nextInt ( 3 ); // 随机整数分区
                long tmstp = System.currentTimeMillis ( ); // 消息时间戳

                JSONPObject value = new JSONPObject (String.format ( "{'name':'%s','num':%d}",key+"",key ));
                ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, partition,tmstp,key,value.toJSONString ());
                // 注册回调，消息成功发送，收到 leader ack 响应后，执行回调
                producer.send(record,new Callback (){
                    @Override
                    public void onCompletion ( RecordMetadata recordMetadata , Exception e ) {
                        int partition = recordMetadata.partition ( );
                        long offset = recordMetadata.offset ( );
                        int keySize = recordMetadata.serializedKeySize ( );
                        int valueSize = recordMetadata.serializedValueSize ( );
                        long tmstp = recordMetadata.timestamp ( );
                        String topic1 = recordMetadata.topic ( );
                        long checksum = recordMetadata.checksum ( );
                        System.out.println ( String.format (
                                "checksum: %s ,topic: %s, tmstp: %s, partition: %s, offset: %s, keySize: %s, valueSize: %s",
                                checksum, topic1, tmstp, partition, offset,keySize , valueSize
                        ) );
                    }
                });
                Thread.sleep ( 100 );
            }
        } catch (Exception e) {
            e.printStackTrace ( );
        } finally {
            if(null!=producer){
                producer.close ();
            }
        }
    }



    public static void main(String[] args) {
        String topic = "jsonRecord";
//        keyedRecord(topic);
        Thread thread1 = new Thread ( new Runnable ( ) {
             @Override
             public void run () {
                 autoCommitConsume (topic);
             }
         } );
        thread1.start ();

        Thread thread2 = new Thread ( new Runnable ( ) {
            @Override
            public void run () {
                jsonRecord(topic);
            }
        } );
        thread2.start ();

    }

}
