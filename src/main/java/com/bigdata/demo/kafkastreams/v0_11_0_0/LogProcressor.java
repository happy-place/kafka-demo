package com.bigdata.demo.kafkastreams.v0_11_0_0;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcressor implements Processor<byte[],byte[]> {

    private ProcessorContext context;

    @Override
    public void init ( ProcessorContext processorContext ) {
        this.context = processorContext;
    }

    @Override
    public void process ( byte[] bytes , byte[] bytes2 ) {
        String value = new String(bytes2);
        String sep = ">>>";
        if(value.contains (sep)){
            value = value.split ( sep )[1].trim ( );
        }
        this.context.forward ( bytes,value.getBytes ());
    }

    @Override
    public void punctuate ( long l ) {

    }

    @Override
    public void close () {

    }
}
