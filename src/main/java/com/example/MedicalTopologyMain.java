package com.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class MedicalTopologyMain {

    public static void main(String[] args) throws Exception {
        // --- uruchomienie serwera WebSocket ---
        MedWebSocketServer.initialize(8887);
        System.out.println("=== Otwórz dashboard.html w przeglądarce, aby zobaczyć alerty na żywo ===");

        // --- budowa topologii ---
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("medical-spout", new MedicalDataSpout(), 1);
        builder.setBolt("alert-bolt", new MedicalAlertBolt(), 1)
               .shuffleGrouping("medical-spout");

        Config config = new Config();
        config.setDebug(false);

        // --- uruchomienie klastra lokalnego ---
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("medical-alert-topology", config, builder.createTopology());

        System.out.println("=== Topologia uruchomiona. Działa przez 60 sekund... ===");
        Thread.sleep(60_000);

        // --- zamknięcie ---
        cluster.killTopology("medical-alert-topology");
        cluster.shutdown();
        MedWebSocketServer.getInstance().stop();
        System.out.println("=== Topologia zatrzymana. ===");
    }
}
