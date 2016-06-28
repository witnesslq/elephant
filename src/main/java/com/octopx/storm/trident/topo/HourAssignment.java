package com.octopx.storm.trident.topo;

import com.octopx.storm.trident.spout.DiagnosisEvent;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yuyang on 16/5/24.
 */
public class HourAssignment extends BaseFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HourAssignment.class);
    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        DiagnosisEvent diagnosisEvent = (DiagnosisEvent) tridentTuple.getValue(0);
        String city = (String) tridentTuple.getValue(1);

        long timestamp = diagnosisEvent.time;
        long hourSinceEpoch = timestamp / 1000 / 60 / 60;

        LOG.debug("Key = [" + city + ":" + hourSinceEpoch + "]");
        String key = city + ":" + diagnosisEvent.diagnosisCode + ":" + hourSinceEpoch;

        List<Object> values = new ArrayList<>();
        values.add(hourSinceEpoch);
        values.add(key);
        tridentCollector.emit(values);
    }
}
