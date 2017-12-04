package com.intellect.ingestion.ingestion;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.intellect.ingestion.model.Book;
import com.intellect.ingestion.service.ElasticSearchService;

@EnableKafka
@Component
public class Ingestion {

	@Autowired
	private ElasticSearchService elasticSearchService;

	@KafkaListener(topics = "${kafka.topic.name}", group = "${kafka.consumer.groupid}", containerFactory = "kafkaListenerContainerFactory")
	public void receiveMessage(Book message) throws JsonMappingException, IOException, NullPointerException {

		System.out.println("***Receiving Kafka Message..." + message);

		if (message.getOperation().equalsIgnoreCase("CREATE")) {

			System.out.println("Inside CONDITION -" + message.getOperation());
			elasticSearchService.insertIntoElasticSearch(message);
		} else if (message.getOperation().equalsIgnoreCase("DELETE")) {

			System.out.println("Inside CONDITION -" + message.getOperation());
			elasticSearchService.deleteBookById(message.getId());
		} else if ((message.getOperation().equalsIgnoreCase("UPDATE"))) {

			System.out.println("Inside CONDITION -" + message.getOperation());
			elasticSearchService.updateBook(message);
		} else if ((message.getOperation().equalsIgnoreCase("GETALL"))) {

			System.out.println("Inside CONDITION -" + message.getOperation());
			String response = elasticSearchService.getAll();
			System.out.println("" + response);
		} else if ((message.getOperation().equalsIgnoreCase("GET"))) {

			System.out.println("Inside CONDITION -" + message.getOperation());
			String response = elasticSearchService.getBookById(message.getId());
			System.out.println("" + response);
		}
	}
}
