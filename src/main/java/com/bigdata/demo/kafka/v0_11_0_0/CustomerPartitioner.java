package com.bigdata.demo.kafka.v0_11_0_0;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class CustomerPartitioner implements Partitioner {

    @Override
    public int partition ( String topic , Object key , byte[] keyBytes , Object value , byte[] valueBytes , Cluster cluster ) {
        int size = cluster.partitionsForTopic ( topic ).size ( );
        int num = key.hashCode ( ) % size;
        return num;
    }

    @Override
    public void close () {

    }

    @Override
    public void configure ( Map<String, ?> map ) {
    }
}
