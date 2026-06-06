package com.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class MedicalAlertBolt extends BaseRichBolt {

    private static final int THROUGHPUT_INTERVAL = 10_000;

    // progi dla reguł medycznych
    private static final int    HIGH_BP          = 140;   // ciśnienie skurczowe [mmHg]
    private static final int    HIGH_CHOL        = 240;   // cholesterol [mg/dl]
    private static final int    LOW_MAX_HR       = 100;   // maksymalny tętno [bpm]
    private static final double HIGH_ST_DEPR     = 2.0;   // obniżenie ST [mm]

    private OutputCollector collector;

    private long tupleCount      = 0;
    private long anomalyCount    = 0;
    private long errorCount      = 0;
    private long globalStart     = 0;
    private long windowStart     = 0;
    private double totalLatency  = 0;
    private double globalLatency = 0;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.globalStart = System.currentTimeMillis();
        this.windowStart = this.globalStart;
    }

    @Override
    public void execute(Tuple tuple) {
        // --- odczyt pól krotki ---
        int    age      = tuple.getIntegerByField("age");
        int    sex      = tuple.getIntegerByField("sex");
        int    cp       = tuple.getIntegerByField("cp");
        int    trestbps = tuple.getIntegerByField("trestbps");
        int    chol     = tuple.getIntegerByField("chol");
        int    thalach  = tuple.getIntegerByField("thalach");
        int    exang    = tuple.getIntegerByField("exang");
        double oldpeak  = tuple.getDoubleByField("oldpeak");
        int    ca       = tuple.getIntegerByField("ca");
        int    target   = tuple.getIntegerByField("target");
        long   emitTime = tuple.getLongByField("timestamp");

        // --- pomiar opóźnienia ---
        long now     = System.currentTimeMillis();
        long latency = now - emitTime;
        totalLatency += latency;
        globalLatency += latency;
        tupleCount++;

        // --- detekcja anomalii medycznych ---
        StringBuilder alert = new StringBuilder();

        if (trestbps >= HIGH_BP) {
            alert.append(String.format("[WYSOKIE CISNIENIE: %d mmHg] ", trestbps));
        }
        if (chol >= HIGH_CHOL) {
            alert.append(String.format("[WYSOKI CHOLESTEROL: %d mg/dl] ", chol));
        }
        if (thalach <= LOW_MAX_HR) {
            alert.append(String.format("[NISKIE TETNO MAKS: %d bpm] ", thalach));
        }
        if (oldpeak >= HIGH_ST_DEPR) {
            alert.append(String.format("[OBNIENIE ST: %.1f mm] ", oldpeak));
        }
        if (exang == 1) {
            alert.append("[BOL W KLATCE PRZY WYSILKU] ");
        }
        if (ca >= 2) {
            alert.append(String.format("[ZWEZENIE NACZYN: %d] ", ca));
        }

        boolean anomaly = alert.length() > 0;
        if (anomaly) anomalyCount++;
        String alertMessage = anomaly ? alert.toString().trim() : "BRAK ANOMALII";

        // --- wypisanie alertu medycznego + wysłanie do dashboardu ---
        if (anomaly) {
            String logMsg = String.format("[ALERT] wiek=%d plec=%s cel=%d | %s | latencja=%dms",
                    age, sex == 1 ? "M" : "K", target, alertMessage, latency);
            System.out.println(logMsg);

            // wysyłamy JSON do przeglądarki
            MedWebSocketServer ws = MedWebSocketServer.getInstance();
            if (ws != null) {
                String json = String.format(
                    "{\"age\":%d,\"sex\":\"%s\",\"trestbps\":%d,\"chol\":%d,\"thalach\":%d,"
                  + "\"oldpeak\":%.1f,\"exang\":%d,\"ca\":%d,\"target\":%d,"
                  + "\"alert\":\"%s\",\"latency\":%d,\"count\":%d,\"anomalyCount\":%d}",
                    age, sex == 1 ? "M" : "K", trestbps, chol, thalach,
                    oldpeak, exang, ca, target,
                    alertMessage.replace("\"", "'"), latency, tupleCount, anomalyCount);
                ws.broadcastAlert(json);
            }
        }

        // --- pomiar przepustowości co THROUGHPUT_INTERVAL krotek ---
        if (tupleCount % THROUGHPUT_INTERVAL == 0) {
            long elapsed = System.currentTimeMillis() - windowStart;
            double throughput  = (elapsed > 0) ? (THROUGHPUT_INTERVAL * 1000.0 / elapsed) : 0;
            double avgLatency  = totalLatency / THROUGHPUT_INTERVAL;

            System.out.printf("%n=== STATYSTYKI (po %,d krotkach) ===%n", tupleCount);
            System.out.printf("  Przepustowosc : %.0f krotek/s%n", throughput);
            System.out.printf("  Sr. opoznienie: %.3f ms%n", avgLatency);
            System.out.printf("======================================%n%n");

            // reset okna pomiarowego
            windowStart  = System.currentTimeMillis();
            totalLatency = 0;
        }

        // --- emituj przetworzoną krotkę dalej ---
        collector.emit(tuple, new Values(age, sex, cp, trestbps, chol, thalach,
                exang, oldpeak, ca, target, anomaly, alertMessage, latency));
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        long totalTime = System.currentTimeMillis() - globalStart;
        double seconds = totalTime / 1000.0;
        double avgThroughput = (seconds > 0) ? (tupleCount / seconds) : 0;
        double avgLatency = (tupleCount > 0) ? (globalLatency / tupleCount) : 0;

        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║           PODSUMOWANIE TOPOLOGII                    ║");
        System.out.println("╠══════════════════════════════════════════════════════╣");
        System.out.printf( "║  Czas dzialania       : %.1f s                     %n", seconds);
        System.out.printf( "║  Przetworzone krotki  : %,d                        %n", tupleCount);
        System.out.printf( "║  Wykryte anomalie     : %,d                        %n", anomalyCount);
        System.out.printf( "║  Bledy przetwarzania  : %,d                        %n", errorCount);
        System.out.printf( "║  Sr. przepustowosc    : %,.0f krotek/s             %n", avgThroughput);
        System.out.printf( "║  Sr. opoznienie       : %.3f ms                    %n", avgLatency);
        System.out.println("╠══════════════════════════════════════════════════════╣");
        if (errorCount == 0) {
            System.out.println("║  STATUS: OK — brak bledow przetwarzania            ║");
        } else {
            System.out.printf( "║  STATUS: UWAGA — %d bledow przetwarzania!          %n", errorCount);
        }
        System.out.printf( "║  ANOMALIE: %.1f%% rekordow z alertem medycznym       %n",
                tupleCount > 0 ? (anomalyCount * 100.0 / tupleCount) : 0);
        System.out.println("╚══════════════════════════════════════════════════════╝");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "age", "sex", "cp", "trestbps", "chol", "thalach",
                "exang", "oldpeak", "ca", "target", "anomaly", "alertMessage", "latency"
        ));
    }
}
