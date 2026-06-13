package com.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

// Klasa startowa , która uruchamia cały system w odpowiedniej kolejności
public class MedicalTopologyMain {

    public static void main(String[] args) throws Exception {
        // najpierw uruchamia serwer Websocket, który posłuży do komunikacji z dashboardem
        MedWebSocketServer.initialize(8887);
        // buduje topologię Storm, definiując spout i bolty oraz sposób ich połączenia
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("medical-spout", new MedicalDataSpout(), 1);
        builder.setBolt("alert-bolt", new MedicalAlertBolt(), 1)
               .shuffleGrouping("medical-spout");
        builder.setBolt("csv-writer-bolt", new CsvWriterBolt(), 1)
                .globalGrouping("alert-bolt");

        // konfiguracja i uruchomienie lokalnego klastra Storm
        Config config = new Config();
        config.setDebug(false);
        
        // uruchomienie topologii w lokalnym klastrze Storm
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("medical-alert-topology", config, builder.createTopology());

        // topologia działa przez 60 sekund, a następnie jest zatrzymywana
        System.out.println("Topologia uruchomiona.");
        Thread.sleep(60_000);

        cluster.killTopology("medical-alert-topology");
        cluster.shutdown();

        // połączenie WebSocket jest również zamykane
        MedWebSocketServer.getInstance().stop();
        System.out.println("Topologia zatrzymana.");
    }
}
