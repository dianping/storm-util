package com.dianping.cosmos.blackhole;


public class BlackholeMessageId {
    private static String MSG_SEPARATOR = "#";
    
    public static String getMessageId(String partition, long offset){
        return partition + MSG_SEPARATOR +  offset;
    }
    
    public static Long getOffset(String msgId){
        String[] message = msgId.split(MSG_SEPARATOR);
        if(message.length == 2){
            return Long.parseLong(message[1]);
        }
        return null;
    }
    
    public static String getPartition(String msgId){
        String[] message = msgId.split(MSG_SEPARATOR);
        if(message.length == 2){
            return message[0];
        }
        return null;
    }
}
