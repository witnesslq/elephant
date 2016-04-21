package com.guahao.spring.jdbc;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

// This ApplicationRunner class implements Spring Boot's CommandLineRunner,
// which means it will execute the run() method after the application context is loaded up.
public class JdbcRunner implements CommandLineRunner {
	private static final Logger logger = LoggerFactory.getLogger(JdbcRunner.class);
	
	// Spring Boot spots H2, an in-memory relational database engine, and automatically
	// creates a connection. Because we are using spring-jdbc, Spring Boot automatically creates a
	// JdbcTemplate. The @Autowired JdbcTemplate field automatically loads it and makes it available.
	@Autowired
	JdbcTemplate jdbcTemplate;
	
	public void run(String... arg0) throws Exception {
		logger.info("Application Starting....");
		logger.info("Creating tables");
		
		jdbcTemplate.execute("DROP TABLE customers IF EXISTS");
		jdbcTemplate.execute("CREATE TABLE customers(" + 
							"id SERIAL, first_name VARCHAR(255), last_name VARCHAR(255))");
		
		// Split up the array of whole names into an array of first/last names
		List<Object[]> splitUpNames = Arrays.asList("John Woo", "Jeff Dean", "Josh Bloch", "Josh Long").stream()
										.map(name -> name.split(" "))
										.collect(Collectors.toList());
		
		// Use a Java 8 stream to print out each tuple of the list
		splitUpNames.forEach(name -> logger.info(String.format("Inserting customer record for %s %s", name[0], name[1])));
		
		// Uses JdbcTemplate's batchUpdate operation to bulk load data
		jdbcTemplate.batchUpdate("INSERT INTO customers(first_name, last_name) VALUES(?, ?)", splitUpNames);
		
		logger.info("Querying for customer records where first_name = 'Josh':");
		jdbcTemplate.query("SELECT id, first_name, last_name FROM customers WHERE first_name = ?", 
				new Object[] { "Josh" },
				(rs, rowNum) -> new Customer(rs.getLong("id"), rs.getString("first_name"), rs.getString("last_name"))
		).forEach(customer -> logger.info(customer.toString()));
	}
}