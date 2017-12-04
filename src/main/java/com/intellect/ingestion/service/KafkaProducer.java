package com.intellect.ingestion.service;

import org.springframework.beans.factory.annotation.Autowired; 
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.intellect.ingestion.model.Book;


@Service
public class KafkaProducer {

	@Autowired
	private KafkaTemplate<String, Book> kafkaTemplate;

	@Value("${kafka.topic.name}")
	private String TOPIC_NAME;

	public String sendMessage(Book book) {

		kafkaTemplate.send(TOPIC_NAME, book);

		return "" + book;

	}

}
