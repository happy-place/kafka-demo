package com.bigdata.demo.kafkastreams.v0_11_0_0;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class Demo {

    private static void logCleanProcessor(){
        String fromTopic = "streams_source";
        String toTopic = "streams_sink";
        String sourceName = "log-clean-source";
        String processorName = "log-clean-processor";
        String sinkName = "log-clean-sink";

        Properties settings = new Properties (  );
        settings.put ( StreamsConfig.APPLICATION_ID_CONFIG,"log clean app" );
        settings.put ( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop01:9092" ); // kafka broker list

        StreamsConfig streamsConfig = new StreamsConfig ( settings );

        TopologyBuilder topologyBuilder = new TopologyBuilder ( );

        topologyBuilder.addSource ( sourceName,fromTopic )
                .addProcessor ( processorName , new ProcessorSupplier ( ) {
                    @Override
                    public Processor get () {
                        return new LogProcressor ();
                    }
                },sourceName )
                .addSink ( sinkName,toTopic,processorName );
        KafkaStreams kafkaStreams = new KafkaStreams ( topologyBuilder , streamsConfig );
        kafkaStreams.start ();

    }

    public static void main ( String[] args ) {
        logCleanProcessor();
    }


}
