package com.example;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class MedicalDataSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private BufferedReader reader;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        openReader();
    }

    private void openReader() {
        try {
            InputStream is = getClass().getClassLoader().getResourceAsStream("heart.csv");
            if (is == null) {
                throw new RuntimeException("Nie znaleziono pliku heart.csv w zasobach");
            }
            reader = new BufferedReader(new InputStreamReader(is));
            reader.readLine(); // pominięcie nagłówka
        } catch (IOException e) {
            throw new RuntimeException("Błąd otwarcia pliku CSV", e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            String line = reader.readLine();

            if (line == null) {
                // koniec pliku – resetujemy czytnik i zaczynamy od nowa
                reader.close();
                openReader();
                line = reader.readLine();
            }

            if (line == null || line.trim().isEmpty()) {
                return;
            }

            String[] fields = line.split(",");
            if (fields.length < 14) {
                return;
            }

            int age        = Integer.parseInt(fields[0].trim());
            int sex        = Integer.parseInt(fields[1].trim());
            int cp         = Integer.parseInt(fields[2].trim());
            int trestbps   = Integer.parseInt(fields[3].trim());
            int chol       = Integer.parseInt(fields[4].trim());
            int fbs        = Integer.parseInt(fields[5].trim());
            int restecg    = Integer.parseInt(fields[6].trim());
            int thalach    = Integer.parseInt(fields[7].trim());
            int exang      = Integer.parseInt(fields[8].trim());
            double oldpeak = Double.parseDouble(fields[9].trim());
            int slope      = Integer.parseInt(fields[10].trim());
            int ca         = Integer.parseInt(fields[11].trim());
            int thal       = Integer.parseInt(fields[12].trim());
            int target     = Integer.parseInt(fields[13].trim());

            long timestamp = System.currentTimeMillis();
            collector.emit(new Values(age, sex, cp, trestbps, chol, fbs,
                    restecg, thalach, exang, oldpeak, slope, ca, thal, target, timestamp));

        } catch (IOException e) {
            throw new RuntimeException("Błąd odczytu pliku CSV", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "age", "sex", "cp", "trestbps", "chol", "fbs",
                "restecg", "thalach", "exang", "oldpeak", "slope", "ca", "thal", "target", "timestamp"
        ));
    }

    @Override
    public void close() {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                // ignorujemy błąd przy zamykaniu
            }
        }
    }
}
