package com.octopx.hadoop.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.IOException;

/**
 * Created by yuyang on 16/7/1.
 */
public class SeqWriter {
    public static void main(String[] args) throws IOException {
        String[] data = {"a,b,c,d,e,f,g", "h,i,j,k,l,m,n", "o,p,q,r,s,t",
            "u,v,w,x,y,z", "0,1,2,3,4,5", "6,7,8,9,10"};
        CompressionCodec codec = new GzipCodec();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("world.seq");
        SequenceFile.Writer writer = null;
        BytesWritable bw = null;
        try {
            IntWritable key = new IntWritable();
            Text value = new Text();
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
            for (int i = 0; i < 10000; i++) {
                key.set(i);
                value.set(data[i % data.length]);
                writer.append(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}