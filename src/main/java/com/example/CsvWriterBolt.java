package com.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.*;
import java.util.Map;


// CsvWriterBolt to końcowy bolt (komponent przetwarzający) w topologii, który zapisuje wyniki przetwarzania do pliku results/heart_results.csv.
public class CsvWriterBolt extends BaseRichBolt { // BaseRichBolt to klasa bazowa z Apache Storm

    private transient PrintWriter csvWriter;
    private OutputCollector collector;

    private long tupleCount = 0;
    private long anomalyCount = 0;

// Funkcje prepare, execute i cleanup musimy nadpisać aby móc dodać swoją logikę inicjalizacji 
    @Override
    // Funkcja prepare jest wywoływana podczas inicjalizacji bolta. Tutaj tworzymy katalog results (jeśli nie istnieje) i otwieramy plik heart_results.csv do zapisu. 
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        try {
            File dir = new File("results");
            if (!dir.exists()) {
                dir.mkdirs();
            }

            File csvFile = new File(dir, "heart_results.csv");

            csvWriter = new PrintWriter(new FileWriter(csvFile, false));

            csvWriter.println("timestamp,age,sex,cp,trestbps,chol,thalach,exang,oldpeak,ca,target,anomaly,alertMessage,latencyMs,tupleCount,anomalyCount");
            csvWriter.flush();

            System.out.println("Zapis wynikow do pliku: " + csvFile.getAbsolutePath());

        } catch (IOException e) {
            throw new RuntimeException("Nie mozna utworzyc pliku CSV z wynikami", e);
        }
    }

    // dla każdej odebranej krotki zapisuje wiersz z danymi pacjenta, flagą anomalii, alertem i latencją do pliku CSV
    @Override
    public void execute(Tuple input) {
        try {
            tupleCount++;

            boolean anomaly = input.getBooleanByField("anomaly");

            if (anomaly) {
                anomalyCount++;
            }

            long timestamp = System.currentTimeMillis();

            String line = String.join(",",
                    String.valueOf(timestamp),
                    String.valueOf(input.getIntegerByField("age")),
                    escapeCsv(input.getStringByField("sex")),
                    String.valueOf(input.getIntegerByField("cp")),
                    String.valueOf(input.getIntegerByField("trestbps")),
                    String.valueOf(input.getIntegerByField("chol")),
                    String.valueOf(input.getIntegerByField("thalach")),
                    String.valueOf(input.getIntegerByField("exang")),
                    String.valueOf(input.getDoubleByField("oldpeak")),
                    String.valueOf(input.getIntegerByField("ca")),
                    String.valueOf(input.getIntegerByField("target")),
                    String.valueOf(anomaly),
                    escapeCsv(input.getStringByField("alertMessage")),
                    String.valueOf(input.getLongByField("latency")),
                    String.valueOf(tupleCount),
                    String.valueOf(anomalyCount)
            );

            csvWriter.println(line);
            csvWriter.flush();

            collector.ack(input);

        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(input);
        }
    }

    private String escapeCsv(String value) {
        if (value == null) {
            return "";
        }

        String escaped = value.replace("\"", "\"\"");

        if (escaped.contains(",") || escaped.contains("\"") || escaped.contains("\n")) {
            return "\"" + escaped + "\"";
        }

        return escaped;
    }

    // przy zamykaniu topologii wykonuje końcowy zapis i zamyka strumień
    @Override
    public void cleanup() {
        if (csvWriter != null) {
            csvWriter.flush();
            csvWriter.close();
        }
    }
    
// Storm wymaga deklarowania declareOutputFields mimo tego, ze bolt nie emituje nic dalej. Bez tej metody kod się nie skompiluje. 
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
}
}