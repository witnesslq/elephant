package com.octopx.storm.trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yuyang on 16/5/24.
 */
public class DiseaseFilter extends BaseFilter {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DiseaseFilter.class);

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        DiagnosisEvent diagnosisEvent = (DiagnosisEvent) tridentTuple.getValue(0);
        Integer code = Integer.parseInt(diagnosisEvent.diagnosisCode);
        if (code.intValue() <= 322) {
            LOG.debug("Emitting disease [" + diagnosisEvent.diagnosisCode + "]");
            return true;
        } else {
            LOG.debug("Filtering disease [" + diagnosisEvent.diagnosisCode + "]");
            return false;
        }
    }
}
