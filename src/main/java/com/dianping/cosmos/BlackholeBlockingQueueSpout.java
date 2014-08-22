package com.dianping.cosmos;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    public static final Log LOG = LogFactory.getLog(BlackholeBlockingQueueSpout.class);
    private final int MAX_QUEUE_SIZE = 1000;
    private final int TIME_OUT = 5000;
    private SpoutOutputCollector collector;
    private String topic;
    private String group;
    private MessageStream stream;
    private Consumer consumer;
    private BlockingQueue<String> emitQueue;
    private Thread fetchThread;

    public BlackholeBlockingQueueSpout(String topic, String group) {
        this.topic = topic;
        this.group = group;
        this.emitQueue = new LinkedBlockingQueue<String>(MAX_QUEUE_SIZE);
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
        stream = consumer.getStream();
        fetchThread = new Thread(new Handler());
        fetchThread.start();
    }

    @Override
    public void close() {
        fetchThread.interrupt();
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
        String message;
        message = emitQueue.poll();
        if (message != null) {
            collector.emit(topic, new Values(message));
        } else {
            Utils.sleep(100);
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
    
    class Handler extends Thread {
        private boolean running;
        public Handler() {
            this.running = true;
            this.setDaemon(true);
            this.setName("Emit-handler");
        }
        
        @Override
        public void run() {
            while (running) {
                for (String message : stream) {
                    try {
                        while(!emitQueue.offer(message, TIME_OUT, TimeUnit.MILLISECONDS)) {
                            LOG.error("Queue is full, cannot offer message.");
                        }
                    } catch (InterruptedException e) {
                        LOG.error("Thread Interrupted");
                        running = false;
                    }
                }
            }
        }
    }
}
