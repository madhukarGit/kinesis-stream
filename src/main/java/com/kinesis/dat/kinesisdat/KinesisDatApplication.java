package com.kinesis.dat.kinesisdat;

import com.kinesis.dat.kinesisdat.service.DatReaderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KinesisDatApplication implements CommandLineRunner {

	@Autowired
	private DatReaderService datReaderService;

	public static void main(String[] args) {
		SpringApplication.run(KinesisDatApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		datReaderService.writeToKinesis();
	}
}
