package com.guahao.spring.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WebApplication {
	private final static Logger logger = LoggerFactory.getLogger(WebApplication.class);
	
	public WebApplication() {
		logger.info("Starting web application.....");
	}
}
