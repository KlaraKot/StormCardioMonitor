package com.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Locale;

import java.util.Map;

public class MedicalAlertBolt extends BaseRichBolt {

    private static final int THROUGHPUT_INTERVAL = 10_000;
    private static final int    HIGH_BP      = 140;
    private static final int    HIGH_CHOL    = 240;
    private static final int    LOW_MAX_HR   = 100;
    private static final double HIGH_ST_DEPR = 2.0;

    private OutputCollector collector;

    private long tupleCount      = 0;
    private long anomalyCount    = 0;
    private long windowStart     = 0;
    private double totalLatency  = 0;

    private PrintWriter csvWriter;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.windowStart = System.currentTimeMillis();

        try {
            File dir = new File("results");
            if (!dir.exists()) {
                dir.mkdirs();
            }

            File csvFile = new File(dir, "heart_results.csv");
            boolean fileExists = csvFile.exists();

            csvWriter = new PrintWriter(new FileWriter(csvFile, false));

            if (!fileExists) {
                csvWriter.println("timestamp,age,sex,cp,trestbps,chol,thalach,exang,oldpeak,ca,target,anomaly,alertMessage,latencyMs,tupleCount,anomalyCount");
                csvWriter.flush();
            }

        } catch (IOException e) {
            throw new RuntimeException("Nie mozna utworzyc pliku CSV z wynikami", e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
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

        long now     = System.currentTimeMillis();
        long latency = now - emitTime;
        totalLatency += latency;
        tupleCount++;

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

        if (csvWriter != null) {
            csvWriter.printf(Locale.US,
                    "%d,%d,%s,%d,%d,%d,%d,%d,%.1f,%d,%d,%b,\"%s\",%d,%d,%d%n",
                    now,
                    age,
                    sex == 1 ? "M" : "K",
                    cp,
                    trestbps,
                    chol,
                    thalach,
                    exang,
                    oldpeak,
                    ca,
                    target,
                    anomaly,
                    alertMessage.replace("\"", "'"),
                    latency,
                    tupleCount,
                    anomalyCount
            );
            csvWriter.flush();
        }

        if (anomaly) {
            String logMsg = String.format("[ALERT] wiek=%d plec=%s cel=%d | %s | latencja=%dms",
                    age, sex == 1 ? "M" : "K", target, alertMessage, latency);
            System.out.println(logMsg);

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

        if (tupleCount % THROUGHPUT_INTERVAL == 0) {
            long elapsed = System.currentTimeMillis() - windowStart;
            double throughput = (elapsed > 0) ? (THROUGHPUT_INTERVAL * 1000.0 / elapsed) : 0;
            double avgLatency = totalLatency / THROUGHPUT_INTERVAL;

            System.out.printf("%n=== STATYSTYKI (po %,d krotkach) ===%n", tupleCount);
            System.out.printf("  Przepustowosc : %.0f krotek/s%n", throughput);
            System.out.printf("  Sr. opoznienie: %.3f ms%n", avgLatency);
            System.out.printf("======================================%n%n");

            windowStart  = System.currentTimeMillis();
            totalLatency = 0;
        }

        collector.emit(tuple, new Values(age, sex, cp, trestbps, chol, thalach,
                exang, oldpeak, ca, target, anomaly, alertMessage, latency));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "age", "sex", "cp", "trestbps", "chol", "thalach",
                "exang", "oldpeak", "ca", "target", "anomaly", "alertMessage", "latency"
        ));
    }
}
