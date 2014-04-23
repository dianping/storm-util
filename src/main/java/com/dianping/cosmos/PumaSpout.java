package com.dianping.cosmos;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.dianping.puma.api.ConfigurationBuilder;
import com.dianping.puma.api.EventListener;
import com.dianping.puma.api.PumaClient;
import com.dianping.puma.core.event.ChangedEvent;
import com.dianping.puma.core.event.RowChangedEvent;


public class PumaSpout implements IRichSpout{
    public static final Log LOG = LogFactory.getLog(PumaSpout.class);
    
    private SpoutOutputCollector collector;
    private PumaEventListener listener; 
    private BlockingQueue<RowChangedEvent> receiveQueue;
    
    class PumaEventListener implements EventListener {

        @Override
        public void onEvent(ChangedEvent event) throws Exception {
            if (!(event instanceof RowChangedEvent)) {
                return;
            }
            RowChangedEvent e = (RowChangedEvent)event;
            receiveQueue.add(e);
        }

        @Override
        public boolean onException(ChangedEvent event, Exception e) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public void onConnectException(Exception e) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void onConnected() {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void onSkipEvent(ChangedEvent event) {
            // TODO Auto-generated method stub
            
        }
        
    }
    
    @Override
    public void ack(Object msgId) {
        LOG.info("ack: " + msgId);   
    }

    @Override
    public void activate() {
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void deactivate() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("fail: " + msgId);  
    }

    @Override
    public void nextTuple() {
        RowChangedEvent event = null;
        try {
            event = receiveQueue.take();
        } catch (InterruptedException e) {
            return;
        }
        
        String database = event.getDatabase();
        String table = event.getTable();
        long executeTime = event.getExecuteTime();
        
        String streamId = database + "." + table;
        String messageId = streamId + "." + executeTime;
        
        collector.emit(streamId, new Values(event), messageId);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector _collector) {
        collector = _collector;
        receiveQueue = new LinkedBlockingQueue<RowChangedEvent>();
        
        ConfigurationBuilder configBuilder = new ConfigurationBuilder();
        configBuilder.ddl(false);
        configBuilder.dml(true);
        configBuilder.seqFileBase((String) conf.get("puma.seqFileBase"));
        configBuilder.host("192.168.8.22");
        configBuilder.port(8000);
        configBuilder.serverId(1112);
        configBuilder.name("pumaspout");
        configBuilder.tables("TuanGou2010", "TG_Order", "TG_Receipt");
        configBuilder.target("77_20");
        configBuilder.transaction(false);     
        PumaClient pc = new PumaClient(configBuilder.build());
        
        listener = new PumaEventListener();
        pc.register(listener);
        pc.start();
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("TuanGou2010.TG_Order", new Fields("event"));
        declarer.declareStream("TuanGou2010.TG_Receipt", new Fields("event"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}
