package com.dianping.cosmos.blackhole;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.data.api.DPRedis;
import com.dianping.pigeon.remoting.ServiceFactory;
import com.dp.blackhole.consumer.api.MessagePack;
import com.dp.blackhole.consumer.api.OffsetStrategy;

public class StormOffsetStrategy implements OffsetStrategy {
    public static final Logger LOG = LoggerFactory.getLogger(StormOffsetStrategy.class);

    private static final String TABLE_NAME = "bi.blackhole_offset.dim";
    private static final int EXPIRE = 604800;//一周的过期时间
    private DPRedis dpRedis = ServiceFactory.getService(DPRedis.class, 500);
    
    private String consumerGroup;
    //多少条消息后，同步一次到Redis中
    private int syncFrequency = 20000;
    private int syncCounter = 0;
    private String topic;
    
    private Map<String, String> offsetMap = new HashMap<String, String>();
    
    public void setConsumerGroup(String consumerGroup){
        this.consumerGroup = consumerGroup;
    }
    
    public void setSyncFrequency(int syncFrequency){
        this.syncFrequency = syncFrequency;
    }
    
    public void setTopic(String topic){
        this.topic = topic;
    }
    
    public long getOffset(String topic, String partition, long endOffset, long committedOffset) {
        try {
            String offsetValue = dpRedis.hget(TABLE_NAME, getKey(topic), partition);
            if(offsetValue != null){
                 long offset = Long.valueOf(offsetValue) ;
                 if(offset > endOffset){
                     LOG.error("topic = " + topic + ", partition " + partition 
                             + "'s endOffset less than redis offset value, use endOffset instead!");
                     return endOffset;
                 }
                 return offset;
            }
            return committedOffset;
        } catch (Exception e) {
            LOG.error("get topic = " + topic + ", partition = " + partition 
                    + " offset error, use committedOffset!", e);
        }
        return committedOffset;
    }
    
    public void syncOffset(){
        try {
            if(offsetMap.size() > 0){
                dpRedis.hset(TABLE_NAME, getKey(topic), offsetMap, EXPIRE);
                //写入成功后，重置
                offsetMap = new HashMap<String, String>();
                syncCounter = 0;
            }
        } catch (Exception e) {
            LOG.error("set topic = " + topic + ", consumerGroup = " + consumerGroup 
                    + " offset error", e);
        }
    }
    
    /**
     * 更新最后收到的消息
     * @param message
     */
    public void updateOffset(MessagePack message){
        if(syncFrequency > 0){
            offsetMap.put(message.getPartition(), String.valueOf(message.getOffset()));
            syncCounter++;
            if(syncCounter >= syncFrequency){
                syncOffset();
            }
        }
    }
    
    
    
    private String getKey(String topic){
        return topic + "#" + this.consumerGroup;
    }
}
