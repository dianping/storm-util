package com.dianping.cosmos;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.dianping.cosmos.bolt.PrinterBolt;

public class BlackholeBlockingQueueSpoutTest {
    private static String TOPIC = "testblackhole2";
    private static String SPOUT_ID = "Test";
    
    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_ID, 
                new BlackholeBlockingQueueSpout(TOPIC, "StormCluster", 200), 1);
        builder.setBolt("Print", new PrinterBolt(), 1).shuffleGrouping(SPOUT_ID, TOPIC);        
        
        Config conf = new Config();
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TestBlackhole", conf, builder.createTopology());
    }
}
