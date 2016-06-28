package com.octopx.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

/**
 * Created by yuyang on 16/5/24.
 */
public class OutbreakDetectionTopology {

    public static void main(String[] args) throws InterruptedException {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("crc", conf, buildTopology());
        Thread.sleep(200000);
        cluster.shutdown();
    }

    public static StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();

        DiagnosisEventSpout spout = new DiagnosisEventSpout();
        Stream inputStream = topology.newStream("event", spout);
        inputStream
                // Filter for critical events
                .each(new Fields("event"), new DiseaseFilter())
                // Locate the closet city
                .each(new Fields("event"), new CityAssignment(), new Fields("city"))
                // Derive the hour segment
                .each(new Fields("event", "city"), new HourAssignment(), new Fields("hour", "cityDiseaseHour"))
                // Group occurrences in same city and hour
                .groupBy(new Fields("cityDiseaseHour"));
//                .persistentAggregate(new OutbreakTreadFactory())


        return topology.build();
    }
}
