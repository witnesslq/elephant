package com.octopx.impala.client;

import java.util.List;

import org.junit.Test;

import com.octopx.impala.client.ImpalaJDBC;

import static org.junit.Assert.*;

public class ImpalaJDBCTest {
	
	@Test
	public void testImpalaConnection() {
		ImpalaJDBC impalaJDBC = new ImpalaJDBC();
		String sql = "SELECT * FROM d limit 8";
		List<String> list = impalaJDBC.getResultList(sql);
		assertEquals("Error - you didn't fetch enough rows that we expected", 8, list.size());
	}
}