package com.guahao.bigdata;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

import java.util.Arrays;

import org.hamcrest.core.CombinableMatcher;
import org.junit.Test;

public class AssertTests {

	@Test
	public void testAssertArrayEquals() {
		byte[] expected = "trial".getBytes();
		byte[] actual = "trial".getBytes();
		assertArrayEquals("failure - byte arrays not same", expected, actual);
	}

	@Test
	public void testAssertThatHasItems() {
		assertThat(Arrays.asList("one", "two", "three"), hasItems("one", "two"));
	}
	
	@Test
	public void testAssertThatHamcrestCoreMatchers() {
		assertThat(7, not(CombinableMatcher.<Integer> either(equalTo(3)).or(equalTo(4))));
	}
}
