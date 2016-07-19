package com.octopx.apache.kafka;

import java.io.IOException;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import kafka.utils.Log4jControllerMBean;

public class Monitoring {
	private static final String HOST = "192.168.1.90";
	private static final String PORT = "9393";
	private static final String JMXURL = "service:jmx:rmi:///jndi/rmi://" + HOST + ":" + PORT + "/jmxrmi";
	
	public static void main(String[] args) throws IOException, MalformedObjectNameException {
		JMXServiceURL jmxUrl = new JMXServiceURL(JMXURL);
		JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxUrl);
		MBeanServerConnection mbeanServerConnection = jmxConnector.getMBeanServerConnection();
		
		// 构建Kafka的日志ObjectName
		ObjectName kafkaLog4jController = new ObjectName("kafka:type=kafka.Log4jController");
		
		Log4jControllerMBean log4jControllerMBean = (Log4jControllerMBean) MBeanServerInvocationHandler.
				newProxyInstance(mbeanServerConnection, kafkaLog4jController, Log4jControllerMBean.class, true);
		
		for (String logger : log4jControllerMBean.getLoggers()) {
			System.out.println(logger);
		}
		
		System.out.println("Modify the logger level: " + log4jControllerMBean.setLogLevel("root", "DEBUG"));
		
		for (String logger : log4jControllerMBean.getLoggers()) {
			System.out.println(logger);
		}
		
		System.out.println("MBean Count: " + mbeanServerConnection.getMBeanCount());
	}
}
