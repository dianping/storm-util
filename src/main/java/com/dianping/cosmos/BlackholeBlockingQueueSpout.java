package com.dianping.cosmos;

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

import com.dianping.lion.client.LionException;
import com.dp.blackhole.consumer.Consumer;
import com.dp.blackhole.consumer.ConsumerConfig;
import com.dp.blackhole.consumer.MessageStream;

public class BlackholeBlockingQueueSpout implements IRichSpout {
    private static final long serialVersionUID = 1111L;
    public static final Logger LOG = LoggerFactory.getLogger(BlackholeBlockingQueueSpout.class);
    private SpoutOutputCollector collector;
    private String topic;
    private String group;
    private MessageStream stream;
    private Consumer consumer;
    private MessageFetcher fetchThread;
    private int warnningStep = 0;

    public BlackholeBlockingQueueSpout(String topic, String group) {
        this.topic = topic;
        this.group = group;
    }
    
    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector _collector) {
        collector = _collector;
        ConsumerConfig config = new ConsumerConfig();
        try {
            consumer = new Consumer(topic, group, config);
        } catch (LionException e) {
            throw new RuntimeException(e);
        }
        consumer.start();
        stream = consumer.getStream();
        
        fetchThread = new MessageFetcher(stream);
        new Thread(fetchThread).start();
    }

    @Override
    public void close() {
//        fetchThread.interrupt();
    }

    @Override
    public void activate() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void deactivate() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void nextTuple() {
        String message = fetchThread.pollMessage();
        if (message != null) {
            collector.emit(topic, new Values(message));
        } else {
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
        // TODO Auto-generated method stub
        return null;
    }
}
