package com.intellect.ingestion.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.intellect.ingestion.model.Book;
import com.intellect.ingestion.service.KafkaProducer;

@RestController
public class ApplicationController {

	@Autowired
	private KafkaProducer kafkaProducer;

	@PostMapping("/sendObject")
	public String sendObject(@RequestBody Book book) {

		System.out.println("HelloWorld");
		kafkaProducer.sendMessage(book);

		return "Message sent successfully";
	}
}
