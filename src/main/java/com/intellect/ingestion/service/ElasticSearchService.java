package com.intellect.ingestion.service;

import java.io.IOException; 

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.intellect.ingestion.model.Book;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
 
@Service
public class ElasticSearchService {

	private static JestClient jestClient;

	@Value("${spring.jest.uri}")
	private String jestUri;

	@Value("${spring.jest.type}")
	private String documentType;

	@Value("${spring.jest.index}")
	private String documentIndex;

	@PostConstruct
	public void createConnections() {

		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder(jestUri).build());
		jestClient = factory.getObject();

	}

	public int insertIntoElasticSearch(Book book) throws IOException {

		System.out.println("Check Point 1");
		boolean isIndexExists = jestClient.execute(new IndicesExists.Builder(documentIndex).build()).isSucceeded();

		if (!isIndexExists) {
			System.out.println("Check Point 2");
			jestClient.execute(new CreateIndex.Builder(documentIndex).build());
		}
		Index index = new Index.Builder(book).index(documentIndex).type(documentType).id(book.getId()).build();
		int respCode = jestClient.execute(index).getResponseCode();
		System.out.println("Check Point 3 INSERTED");
		return respCode;
	}
	public void deleteBookById(String id) {

		try {

			jestClient.execute(new Delete.Builder(id).index(documentIndex).type(documentType).build());
		} catch (IOException e) {

			e.printStackTrace();
		}
	}

	public String getAll() throws IOException {

		SearchResult result = jestClient
				.execute(new Search.Builder("").addIndex(documentIndex).addType(documentType).build());
		return result.getJsonString();
	}

	public String getBookById(String id) throws IOException, NullPointerException {

		SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
		searchBuilder.query(QueryBuilders.matchQuery("id", id));
		Search idSearch = new Search.Builder(searchBuilder.toString()).addIndex(documentIndex).addType(documentType)
				.build();
		JestResult searchResult = jestClient.execute(idSearch);

		return searchResult.getValue("hits").toString();
	}

	public int updateBook(Book book) throws IOException {

		Index updateIndex = new Index.Builder(book).index(documentIndex).type(documentType).id(book.getId()).build();
		int respCode = jestClient.execute(updateIndex).getResponseCode();

		return respCode;
	}

	@PreDestroy
	public void closeConnections() {

		try {
			jestClient.close();
		} catch (IOException e) {

			e.printStackTrace();
		}
	}

}
