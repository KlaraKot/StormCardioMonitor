package com.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class MedicalTopologyMain {

    public static void main(String[] args) throws Exception {
        MedWebSocketServer.initialize(8887);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("medical-spout", new MedicalDataSpout(), 1);
        builder.setBolt("alert-bolt", new MedicalAlertBolt(), 1)
               .shuffleGrouping("medical-spout");
        builder.setBolt("csv-writer-bolt", new CsvWriterBolt(), 1)
                .globalGrouping("alert-bolt");

        Config config = new Config();
        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("medical-alert-topology", config, builder.createTopology());

        System.out.println("=== Topologia uruchomiona. Dziala przez 60 sekund... ===");
        Thread.sleep(60_000);

        cluster.killTopology("medical-alert-topology");
        cluster.shutdown();
        MedWebSocketServer.getInstance().stop();
        System.out.println("=== Topologia zatrzymana. ===");
    }
}
