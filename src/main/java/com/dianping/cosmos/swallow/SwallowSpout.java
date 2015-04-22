package com.dianping.cosmos.swallow;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.consumer.Consumer;
import com.dianping.swallow.consumer.ConsumerConfig;
import com.dianping.swallow.consumer.impl.ConsumerFactoryImpl;


@SuppressWarnings({"rawtypes"})
public class SwallowSpout implements IRichSpout {
    private static final long serialVersionUID = 1L;

    public static final Logger LOG = LoggerFactory.getLogger(SwallowSpout.class);
    
    private SpoutOutputCollector collector;
    private String topic;
    private String comsumerId;
    private Consumer consumer;
    private SwallowMessageListener listener;
    private int warnningStep = 0;
    
    public SwallowSpout(String topic, String comsumerId) {
        this.topic = topic;
        this.comsumerId = comsumerId;
    }
    
    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector _collector) {
        collector = _collector;
        ConsumerConfig config = new ConsumerConfig(); 
        consumer = ConsumerFactoryImpl.getInstance().createConsumer(Destination.topic(topic), comsumerId, config);  
        listener = new SwallowMessageListener();
        consumer.setListener(listener);
        consumer.start();
    }

    @Override
    public void close() {
        
    }

    @Override
    public void activate() {        
    }

    @Override
    public void deactivate() { 
        listener.shutdown();
        consumer.close();
    }

    @Override
    public void nextTuple() {
        Message message = listener.pollMessage();
        if (message != null) {
            collector.emit(topic, new Values(message.getContent()), message.getMessageId());
        }
        else{
            Utils.sleep(100);
            warnningStep++;
            if (warnningStep % 100 == 0) {
                LOG.warn("Queue is empty, cannot poll message.");
            }
        }
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("ack: " + msgId);
        
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("fail: " + msgId);   
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(topic, new Fields("event"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
