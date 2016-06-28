package com.octopx.storm.trident;

import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yuyang on 16/5/24.
 */
public class CityAssignment implements Function {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CityAssignment.class);
    private static Map<String, double[]> CITIES = new HashMap<String, double[]>();

    {
        double[] phl = { 39.875365, -75.249524 };
        CITIES.put("PHL", phl);
        double[] nyc = { 40.71448, -74.00598 };
        CITIES.put("NYC", nyc);
        double[] sf = { -31.4250142, -62.0841809 };
        CITIES.put("SF", sf);
        double[] la = { -34.05374, -118.24307 };
        CITIES.put("LA", la);
    };

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        DiagnosisEvent diagnosisEvent = (DiagnosisEvent) tridentTuple.getValue(0);
        double lastDistance = Double.MAX_VALUE;
        String closestCity = "NONE";

        // Find the closest city
        for (Map.Entry<String, double[]> city : CITIES.entrySet()) {
            double R = 6371;    //km
            double x = (city.getValue()[0] - diagnosisEvent.lng) * Math.cos((city.getValue()[0] + diagnosisEvent.lng) / 2);
            double y = (city.getValue()[1] - diagnosisEvent.lat);
            double d = Math.sqrt(x * x + y * y) * R;
            if (d < lastDistance) {
                lastDistance = d;
                closestCity = city.getKey();
            }
        }

        // Emit the value
        List<Object> values = new ArrayList<Object>();
        values.add(closestCity);
        LOG.debug("Closest city to lat=[" + diagnosisEvent.lat + "], lng=[" + diagnosisEvent.lng + "] == [" + closestCity + "], d=[" + lastDistance + "]");
        tridentCollector.emit(values);
    }

    @Override
    public void prepare(Map map, TridentOperationContext tridentOperationContext) {

    }

    @Override
    public void cleanup() {

    }
}
