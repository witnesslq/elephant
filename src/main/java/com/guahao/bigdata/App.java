package com.guahao.bigdata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import com.guahao.spring.jdbc.JdbcRunner;
import com.guahao.spring.web.WebApplication;

@SpringBootApplication
public class App {
	
	public static void main(String[] args) {
		SpringApplication.run(App.class, args);
	}
}