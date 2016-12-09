package com.octopx.mapreduce.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by yuyang on 16/11/24.
 */
public class ToolRunnerDemo extends Configured implements Tool {
    private Configuration conf;

    static {
        System.setProperty("java.security.krb5.conf", "/Users/yuyang/Desktop/krb5.conf");
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        conf = getConf();
        conf.set("fs.defaultFS", "hdfs://192.168.200.138:8020");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.address", "192.168.200.138:8032");
        conf.set("yarn.resourcemanager.resource-tracker.address", "192.168.200.138:8031");
        conf.set("yarn.resourcemanager.scheduler.address", "192.168.200.138:8030");
        conf.set("yarn.resourcemanager.admin.address", "192.168.200.138:8033");
        conf.set("yarn.application.classpath", "$HADOOP_CONF_DIR,"
                + "$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,"
                + "$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,"
                + "$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,"
                + "$YARN_HOME/*,$YARN_HOME/lib/*,"
                + "$HBASE_HOME/*,$HBASE_HOME/lib/*,$HBASE_HOME/conf/*");
        conf.set("mapreduce.jobhistory.address", "192.168.200.138:10020");
        conf.set("mapreduce.jobhistory.webapp.address", "192.168.200.138:19888");
        conf.set("mapred.child.java.opts", "-Xmx1024m");

        conf.set("hadoop.security.authentication", "Simple");

//        UserGroupInformation.setConfiguration(conf);
//        try {
//            UserGroupInformation.loginUserFromKeytab("yuy@DEV.DXY.CN", "/Users/yuyang/Desktop/yuy.keytab");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        System.out.println(UserGroupInformation.getCurrentUser().getAuthenticationMethod());

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(ToolRunnerDemo.class);
        job.setMapperClass(ToolRunnerDemo.TokenizerMapper.class);
        job.setCombinerClass(ToolRunnerDemo.IntSumReducer.class);
        job.setReducerClass(ToolRunnerDemo.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println(job.waitForCompletion(true) ? 0 : 1);

        JobID jobId = job.getJobID();
        System.out.println("Job ID:" + jobId.toString());

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ToolRunnerDemo(), args);
    }
}
