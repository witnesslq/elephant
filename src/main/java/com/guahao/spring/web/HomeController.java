package com.guahao.spring.web;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.guahao.spring.jdbc.Customer;
import com.guahao.spring.jdbc.JdbcRunner;

@RestController
public class HomeController {
	private static final Logger logger = LoggerFactory.getLogger(HomeController.class);
	private static final String template = "Hello, %s!";
	private final AtomicLong counter = new AtomicLong();
	
	@Autowired JdbcTemplate jdbcTemplate;
	
	@RequestMapping("/greeting")
	public Greeting greeting(@RequestParam(value="name", defaultValue="World") String name) {
		return new Greeting(counter.incrementAndGet(), String.format(template, name));
	}
	
	@RequestMapping("/customers")
	public List<Customer> getCustomers() {
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
		
		List<Customer> customers = jdbcTemplate.query("SELECT id, first_name, last_name FROM customers WHERE first_name = ?", 
				new Object[] { "Josh" },
				(rs, rowNum) -> new Customer(rs.getLong("id"), rs.getString("first_name"), rs.getString("last_name"))
		);
		return customers;
	}
}
