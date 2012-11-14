package com.findwise.hydra.admin.rest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;

import com.findwise.hydra.DatabaseConfiguration;
import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.admin.multiple.ConfigurationServiceMultiple;
import com.findwise.hydra.admin.multiple.DocumentsServiceMultiple;
import com.findwise.hydra.admin.multiple.StagesServiceMultiple;
import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoType;

@Configuration
@ComponentScan(basePackages = "com.findwise.hydra.admin.rest")
public class AppConfig {

	private static MongoConnector blogsConnector = new MongoConnector(
			new DatabaseConfiguration() {

				public int getOldMaxSize() {
					return 100;
				}

				public int getOldMaxCount() {
					return 10000;
				}

				public String getNamespace() {
					return "blogs";
				}

				public String getDatabaseUser() {
					return "admin";
				}

				public String getDatabaseUrl() {
					return "localhost";
				}

				public String getDatabasePassword() {
					return "changeme";
				}
			});

	private static MongoConnector communitiesConnector = new MongoConnector(
			new DatabaseConfiguration() {

				public int getOldMaxSize() {
					return 100;
				}

				public int getOldMaxCount() {
					return 10000;
				}

				public String getNamespace() {
					return "communities";
				}

				public String getDatabaseUser() {
					return "admin";
				}

				public String getDatabaseUrl() {
					return "localhost";
				}

				public String getDatabasePassword() {
					return "changeme";
				}
			});

	private static MongoConnector filesConnector = new MongoConnector(
			new DatabaseConfiguration() {

				public int getOldMaxSize() {
					return 100;
				}

				public int getOldMaxCount() {
					return 10000;
				}

				public String getNamespace() {
					return "files";
				}

				public String getDatabaseUser() {
					return "admin";
				}

				public String getDatabaseUrl() {
					return "localhost";
				}

				public String getDatabasePassword() {
					return "changeme";
				}
			});

	private static MongoConnector peopleConnector = new MongoConnector(
			new DatabaseConfiguration() {

				public int getOldMaxSize() {
					return 100;
				}

				public int getOldMaxCount() {
					return 10000;
				}

				public String getNamespace() {
					return "people";
				}

				public String getDatabaseUser() {
					return "admin";
				}

				public String getDatabaseUrl() {
					return "localhost";
				}

				public String getDatabasePassword() {
					return "changeme";
				}
			});

	private static MongoConnector thelibraryConnector = new MongoConnector(
			new DatabaseConfiguration() {

				public int getOldMaxSize() {
					return 100;
				}

				public int getOldMaxCount() {
					return 10000;
				}

				public String getNamespace() {
					return "thelibrary";
				}

				public String getDatabaseUser() {
					return "admin";
				}

				public String getDatabaseUrl() {
					return "localhost";
				}

				public String getDatabasePassword() {
					return "changeme";
				}
			});

	private static MongoConnector wikisConnector = new MongoConnector(
			new DatabaseConfiguration() {

				public int getOldMaxSize() {
					return 100;
				}

				public int getOldMaxCount() {
					return 10000;
				}

				public String getNamespace() {
					return "wikis";
				}

				public String getDatabaseUser() {
					return "admin";
				}

				public String getDatabaseUrl() {
					return "localhost";
				}

				public String getDatabasePassword() {
					return "changeme";
				}
			});

	public static Map<String, DatabaseConnector<MongoType>> getConnectors() {
		Map<String, DatabaseConnector<MongoType>> connectors = new HashMap<String, DatabaseConnector<MongoType>>();

		try {
			blogsConnector.connect();
			connectors.put("blogs", blogsConnector);
			communitiesConnector.connect();
			connectors.put("communities", communitiesConnector);
			filesConnector.connect();
			connectors.put("files", filesConnector);
			peopleConnector.connect();
			connectors.put("people", peopleConnector);
			thelibraryConnector.connect();
			connectors.put("thelibrary", thelibraryConnector);
			wikisConnector.connect();
			connectors.put("wikis", wikisConnector);
		} catch (IOException e) {
			System.err.println("Failed to connect to a MongoDB database");
		}
		return connectors;
	}

	@Bean(name = "multipartResolver")
	public static CommonsMultipartResolver multipartResolver() {
		CommonsMultipartResolver cmr = new CommonsMultipartResolver();

		cmr.setMaxUploadSize(1024 * 1024 * 1024); // 1 Gigabyte...

		return cmr;
	}

	@Bean
	public static PropertyPlaceholderConfigurer properties() {
		PropertyPlaceholderConfigurer ppc = new PropertyPlaceholderConfigurer();
		final Resource[] resources = new ClassPathResource[] {};
		ppc.setLocations(resources);
		ppc.setIgnoreUnresolvablePlaceholders(true);
		return ppc;
	}

	@Bean
	public static ConfigurationServiceMultiple<MongoType> configurationService() {
		return new ConfigurationServiceMultiple<MongoType>(getConnectors());
	}

	@Bean
	public static DocumentsServiceMultiple<MongoType> documentsService() {
		return new DocumentsServiceMultiple<MongoType>(getConnectors());

	}

	@Bean
	public static StagesServiceMultiple<MongoType> stagesService() {
		return new StagesServiceMultiple<MongoType>(getConnectors());
	}

}