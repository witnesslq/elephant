package com.octopx.storm.xlog;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yuyang on 16/7/4.
 */
public class XlogBolt extends BaseBasicBolt {
    private long intervalTime = 60;
    private long totalThreshold = 30;
    private long sqlxssThreshold = 100;
    private long scopeThreshold = 10;
    private String topic = "";
    private String mysqlUrl = "";
    private String mysqlUser = "";
    private String mysqlPassword = "";
    private boolean isStatic = true;
    private int start_datetime = 0;
    private int start_mm = 0;
    private int total = 0, statics = 0, dynamics = 0;
    private int thisTaskId = 0;
    private String[] subcharArr;
    private String[] substrArr;
    private boolean sqlxssEnable = false;
    Hashtable<String, Object> hashIp = new Hashtable<String, Object>(1000, 0.5F);
    Hashtable<String, Object> hashIpUrl = new Hashtable<String, Object>(1000, 0.5F);
    Hashtable<String, Object> hashIpUserAgent = new Hashtable<String, Object>(10, 0.5F);
    HashMap<String, Integer> ipWhitelist = new HashMap<String, Integer>();

    public static int count(String text, String sub) {
        int count = 0, start = 0;
        while ((start = text.indexOf(sub, start)) >= 0) {
            start += sub.length();
            count++;
        }
        return count;
    }

    public static int total(String text, String[] sqlxssChar, String[] sqlxssArr) {
        int x = 0, y = 0, n = 0, m = 0;
        Integer count_char = sqlxssChar.length;
        Integer count_str = sqlxssArr.length;
        for (n = 0; n < count_char; n++) {
            x = x + count(text, sqlxssChar[n]);
        }

        for (m = 0; m < count_str; m++) {
            y = y + 10 * count(text, sqlxssArr[m]);
        }
        return x + y;
    }

    public void prepare(Map stormConf, TopologyContext context) {
        topic = (String) stormConf.get("xlog.kafka.topic.name");
        totalThreshold = Long.parseLong((String) stormConf.get("insert.into.mysql.min.total"), 10);
        sqlxssThreshold = Long.parseLong((String) stormConf.get("insert.into.mysql.min.sqlxss"), 10);
        scopeThreshold = Long.parseLong((String) stormConf.get("insert.into.mysql.max.scope"), 10);
        intervalTime = Long.parseLong((String) stormConf.get("xlog.interval.time"), 10);
        mysqlUrl = (String) stormConf.get("mysql.url");
        mysqlUser = (String) stormConf.get("mysql.user");
        mysqlPassword = (String) stormConf.get("mysql.password");
        subcharArr = StringUtils.split((String) stormConf.get("xlog.sqlxss.char"), ",");
        substrArr = StringUtils.split((String) stormConf.get("xlog.sqlxss.string"), ",");
        String enable = (String) stormConf.get("xlog.sqlxss.enable");
        sqlxssEnable = enable.toLowerCase().equals("true") ? true : false;
        thisTaskId = context.getThisTaskId();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String line = tuple.getString(1);
        String regex = "([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})\\s(.+)\\s\\-\\s\\[(.+)\\s\\+0800\\]\\s\"(.+)\\s(.+)\\s(.+)\"\\s(\\d+)\\s(\\d+)\\s\"(.+)\"\\s\"(.+)\"\\s\"(.+)\"\\s(\\d+\\.\\d+|\\d+|\\-)\\s(\\d+\\.\\d+|\\d+|\\-)";
        String ip = "";
        String host = "-";
        String datetime = "";
        String method = "";
        String url = "";
        String decoudeUrl = "";
        String code = "";
        String size = "";
        String userAgent = "";
        String proxyIp = "-";
        int re_time = 0;
        int mm = 0;
        int sqlxssTotal = 0;
        boolean inWhitelist = false;
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(line);
        while (matcher.find()) {
            ip = matcher.group(1);//只取第一组
            host = matcher.group(2);//只取第一组
            datetime = matcher.group(3);
            method = matcher.group(4);
            url = matcher.group(5);
            code = matcher.group(7);
            size = matcher.group(8);
            userAgent = matcher.group(10);
            proxyIp = matcher.group(11);
            total++;

            //从日志标记提取时间
            TimeZone.setDefault(TimeZone.getTimeZone("GMT+8:00"));
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
            Date d;
            try {
                d = sdf.parse(datetime);
                long l = d.getTime();
                String str = String.valueOf(l);
                re_time = Integer.parseInt(str.substring(0, 10));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            String re_StrTime = null;
            SimpleDateFormat sdf_1 = new SimpleDateFormat("yyyyMMddHHmm", Locale.CHINA);
            SimpleDateFormat sdf_mm = new SimpleDateFormat("mm", Locale.CHINA);
            long lcc_time = Long.valueOf(re_time);
            re_StrTime = sdf_1.format(new Date(lcc_time * 1000L));
            mm = Integer.parseInt(sdf_mm.format(new Date(lcc_time * 1000L)));

            //开始时间标记
            if (start_datetime == 0) {
                start_datetime = re_time;
                start_mm = mm;
            }

            //已经在白名单的不再继续统计
            if (ip.equals("-")) {
                continue;
            }

            inWhitelist = ipWhitelist.containsKey(ip);
            if (inWhitelist) {
                continue;
            }

            //访问广度是否达到白名单阀值，是的话添加到白名单列表，并清空hashIpUrl记录以节省内存
            boolean urlMapIsExist = hashIpUrl.containsKey(ip);
            HashMap<String, Integer> mapUrl = null;
            mapUrl = urlMapIsExist ? (HashMap<String, Integer>) hashIpUrl.get(ip) : new HashMap<String, Integer>();
            Integer mapUrlSize = mapUrl.size();
            if (mapUrlSize >= scopeThreshold) {
                hashIpUrl.remove(ip);
                ipWhitelist.put(ip, 1);
                inWhitelist = true;
            }


            //是否为静态记录
            if (url.toLowerCase().matches(".+(\\.jpg|\\.png|\\.js|\\.gif|\\.css|\\.ico|\\.swf|\\.jpeg|\\.txt|\\.html|\\.htm){1}.*")) {
                statics++;
                isStatic = true;
            } else {
                dynamics++;
                isStatic = false;
            }


            if (!inWhitelist) {
                String newUrl = "";
                if (isStatic) {
                    newUrl = url;
                } else {
                    String[] urlArr = StringUtils.split(url, "/");
                    Integer count = urlArr.length;
                    if (count >= 3) {
                        newUrl = "/" + urlArr[0] + "/" + urlArr[1];
                    } else if (count <= 0) {
                        newUrl = "/";
                    } else {
                        //Integer indexof1 = url.indexOf('.');
                        Integer indexof2 = url.indexOf('?');
                        Integer indexof3 = url.indexOf('=');
                        if (indexof3 != -1 && indexof2 != -1) {
                            newUrl = url.substring(0, indexof3);
                        } else {
                            newUrl = urlArr[0];
                        }
                    }
                }
                mapUrl.put(newUrl, 1);
                hashIpUrl.put(ip, mapUrl);

            }

            HashMap<String, Integer> map = null;
            boolean ipMapIsExist = hashIp.containsKey(ip);
            map = ipMapIsExist ? (HashMap<String, Integer>) hashIp.get(ip) : new HashMap<String, Integer>();
            if (inWhitelist && ipMapIsExist) {
                hashIp.remove(ip);
                continue;
            }

            //代理信息
            boolean userAgentMapIsExist = hashIpUserAgent.containsKey(ip);
            HashMap<String, Integer> mapUserAgent = null;
            mapUserAgent = userAgentMapIsExist ? (HashMap<String, Integer>) hashIpUserAgent.get(ip) : new HashMap<String, Integer>();
            mapUserAgent.put(userAgent, 1);
            hashIpUserAgent.put(ip, mapUserAgent);

            Integer codeFirst = Integer.parseInt(code.substring(0, 1));
            String fieldName = "";

            if (sqlxssEnable) {
                try {
                    decoudeUrl = URLDecoder.decode(url.replaceAll("%", "%25"), "utf-8").toLowerCase();
                    sqlxssTotal = total(decoudeUrl, subcharArr, substrArr);
                } catch (UnsupportedEncodingException e) {
                    //e.printStackTrace();
                    System.out.println("UnsupportedEncodingException:" + url);
                }
            }

            if (ipMapIsExist) {
                map.put("total", map.get("total") + 1);
                if (isStatic) {
                    map.put("statics", map.get("statics") + 1);
                } else {
                    map.put("dynamics", map.get("dynamics") + 1);
                }

                if (method.equals("GET")) {
                    map.put("get", map.get("get") + 1);
                } else if (method.equals("POST")) {
                    map.put("post", map.get("post") + 1);
                } else if (method.equals("HEAD")) {
                    map.put("head", map.get("head") + 1);
                } else {
                    map.put("other", map.get("other") + 1);
                }

                for (int c = 2; c <= 5; c++) {
                    fieldName = c + "xx";
                    if (codeFirst == c) {
                        map.put(fieldName, map.get(fieldName) + 1);
                    }
                }
                if (!proxyIp.equals("-")) {
                    map.put("proxy", map.get("proxy") + 1);
                }

                if (sqlxssEnable) {
                    map.put("sqlxss", map.get("sqlxss") + sqlxssTotal);
                }
            } else {
                map.put("total", 1);
                if (isStatic) {
                    map.put("statics", 1);
                    map.put("dynamics", 0);
                } else {
                    map.put("statics", 0);
                    map.put("dynamics", 1);
                }

                map.put("get", 0);
                map.put("post", 0);
                map.put("head", 0);
                map.put("other", 0);
                if (method.equals("GET")) {
                    map.put("get", 1);
                } else if (method.equals("POST")) {
                    map.put("post", 1);
                } else if (method.equals("HEAD")) {
                    map.put("head", 1);
                } else {
                    map.put("other", 1);
                }

                for (int c = 2; c <= 5; c++) {
                    fieldName = c + "xx";
                    if (codeFirst == c) {
                        map.put(fieldName, 1);
                    } else {
                        map.put(fieldName, 0);
                    }
                }
                if (!proxyIp.equals("-")) {
                    map.put("proxy", 1);
                } else {
                    map.put("proxy", 0);
                }

                if (sqlxssEnable) {
                    map.put("sqlxss", sqlxssTotal);
                } else {
                    map.put("sqlxss", 0);
                }
            }

            if (!inWhitelist && ipMapIsExist && mapUrlSize + 1 >= scopeThreshold) {
                mapUrlSize = mapUrl.size();
                if (mapUrlSize >= scopeThreshold) {
                    ipWhitelist.put(ip, 1);
                    hashIpUrl.remove(ip);
                    hashIp.remove(ip);
                    continue;
                }
            }
            hashIp.put(ip, map);

        /*System.out.println("总数：" + total + ",静态：" + statics + ",动态：" + dynamics);
            System.out.println("#####################################");*/
        }

        if ((mm - start_mm > 0 && re_time - start_datetime > intervalTime) || (mm - start_mm < 0 && re_time - start_datetime > intervalTime)) {
            System.out.println("TaskId: " + thisTaskId + " -> " + line);
            System.out.println("TaskId: " + thisTaskId + " # topic -> " + topic + " totals -> " + total + " # ip totals ->" + hashIp.size() + " # whitelist totals ->" + ipWhitelist.size());
            String data = "";
            ArrayList ipList = new ArrayList();
            long totalIp = 0;
            long totalSqlxss = 0;
            long valid = 0;
            long scopeSize = 0;
            long userAgentSize = 0;
            for (Iterator<String> it = hashIp.keySet().iterator(); it.hasNext(); ) {
                String key = (String) it.next();
                HashMap<String, Integer> value = (HashMap<String, Integer>) hashIp.get(key);
                HashMap<String, Integer> hashUrl = (HashMap<String, Integer>) hashIpUrl.get(key);
                HashMap<String, String> hashUserAgent = (HashMap<String, String>) hashIpUserAgent.get(key);
                totalIp = value.get("total");
                totalSqlxss = value.get("sqlxss");
                scopeSize = hashUrl.size();
                userAgentSize = hashUserAgent.size();
                //System.out.println("proxy: " +value.get("proxy") + " -> " + userAgentSize);
                if ((totalIp > totalThreshold && scopeSize < scopeThreshold) || totalSqlxss > sqlxssThreshold) {
                    valid++;
                    ipList.add("'" + topic + "','" + key + "','" + start_datetime + "','" + re_time + "','" + value.get("total") + "','" + value.get("statics") + "','" + value.get("dynamics") + "','" + value.get("2xx") + "','" + value.get("3xx") + "','" + value.get("4xx") + "','" + value.get("5xx") + "','" + value.get("get") + "','" + value.get("post") + "','" + value.get("head") + "','" + value.get("other") + "','" + scopeSize + "','" + totalSqlxss + "','" + userAgentSize + "','" + value.get("proxy") + "'");
                }
                value = null;
                hashUrl = null;

            }
            data = StringUtils.join(ipList.toArray(), "), (");
            try {
                Class.forName("com.mysql.jdbc.Driver").newInstance();
                Connection conn = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
                Statement stmt = conn.createStatement();//创建语句对象，用以执行sql语言
                if (valid > 0) {
                    String sql = "INSERT INTO  `ips` (`topic`,`ip` ,`time_start` ,`time_end` ,`total` ,`statics` ,`dynamics` ,`2xx` ,`3xx` ,`4xx` ,`5xx` ,`get` ,`post` ,`head` ,`other`, `scope`, `sqlxss`, `useragent`, `proxy`) VALUES (" + data + ");";
                    //System.out.println(sql);
                    stmt.execute(sql);
                    data = "";
                    sql = "";
                }
                conn.close();
            } catch (Exception ex) {
                System.out.println("Error : " + ex.toString());
            }

            total = 0;
            statics = 0;
            dynamics = 0;
            hashIp = new Hashtable<String, Object>(1000, 0.5F);
            hashIpUrl = new Hashtable<String, Object>(1000, 0.5F);
            hashIpUserAgent = new Hashtable<String, Object>(10, 0.5F);
            ipWhitelist = new HashMap<String, Integer>();
            isStatic = true;
            start_datetime = 0;
            start_mm = 0;

        }
    }
}
