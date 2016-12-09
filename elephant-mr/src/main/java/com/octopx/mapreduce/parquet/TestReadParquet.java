package com.octopx.mapreduce.parquet;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.parquet.Log;

/**
 * Created by yuyang on 16/7/5.
 */
public class TestReadParquet extends Configured implements Tool {
    private static final Log LOG = Log.getLog(TestReadParquet.class);

    private static class FieldDescription {
        public String constraint;
        public String type;
        public String name;
    }

    private static class RecordSchema {
        public RecordSchema(String message) {
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }
}
