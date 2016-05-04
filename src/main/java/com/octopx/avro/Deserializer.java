package com.octopx.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

public class Deserializer {
	public static void main(String[] args) throws IOException {
		File file = new File("users.avro");
		
		// Deserailize Users from disk
		DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
		DataFileReader<User> dataFileReader = new DataFileReader<User>(file, userDatumReader);
		User user = null;
		while (dataFileReader.hasNext()) {
			user = dataFileReader.next(user);
			System.out.println(user);
		}
	}
}
