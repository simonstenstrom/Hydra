package com.findwise.hydra.admin.multiple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.admin.database.AdminServiceQuery;
import com.findwise.hydra.admin.database.AdminServiceType;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.SerializationUtils;

public class DocumentsServiceMultiple<T extends DatabaseType> {

	private Map<String, DatabaseConnector<T>> connectors;
	private static Logger logger = LoggerFactory
			.getLogger(DocumentsServiceMultiple.class);
	public DocumentsServiceMultiple(Map<String, DatabaseConnector<T>> connectors) {
		this.connectors = connectors;
		for (DatabaseConnector<T> connector : connectors.values()) {
			try {
				connector.connect();
			} catch (IOException e) {
				logger.error("Failed to connect");
			}
		}
	}

	public Map<String, Object> getNumberOfDocuments(String pipelineName, String jsonQuery) {
		Map<String, Object> ret = new HashMap<String, Object>();

		try {
			DatabaseQuery<AdminServiceType> query = new AdminServiceQuery();
			query.fromJson(jsonQuery);
			ret.put("numberOfDocuments", getNumberOfDocuments(pipelineName, connectors.get(pipelineName).convert(query)));

		} catch (JsonException e) {
			Map<String, String> error = new HashMap<String, String>();
			error.put("Invalid query", jsonQuery);
			ret.put("error", error);
			ret.put("numberOfDocuments", 0);
		}

		return ret;
	}

	public Map<String, Object> getDocuments(String pipelineName, String jsonQuery, int limit, int skip) {
		Map<String, Object> ret = new HashMap<String, Object>();

		try {
			DatabaseQuery<AdminServiceType> query = new AdminServiceQuery();
			query.fromJson(jsonQuery);
			ret.put("documents", getDocuments(pipelineName, connectors.get(pipelineName).convert(query), limit, skip));

		} catch (JsonException e) {
			Map<String, String> error = new HashMap<String, String>();
			error.put("Invalid query", jsonQuery);
			ret.put("error", error);
			ret.put("documents", new ArrayList<DatabaseDocument<T>>());
		}

		return ret;
	}

	public Map<String, Object> discardDocuments(String pipelineName, String jsonQuery, int limit, int skip) {
		Map<String, Object> ret = new HashMap<String, Object>();

		try {
			DatabaseQuery<AdminServiceType> query = new AdminServiceQuery();
			query.fromJson(jsonQuery);
			ret.put("discarded", discardDocuments(pipelineName, connectors.get(pipelineName).convert(query), limit, skip));

		} catch (JsonException e) {
			Map<String, String> error = new HashMap<String, String>();
			error.put("Invalid query", jsonQuery);
			ret.put("error", error);
			ret.put("discarded", new ArrayList<DatabaseDocument<T>>());
		}

		return ret;
	}
	
	private long getNumberOfDocuments(String pipelineName, DatabaseQuery<T> query) {
		return connectors.get(pipelineName).getDocumentReader().getNumberOfDocuments((DatabaseQuery<T>) query);
	}

	private List<DatabaseDocument<T>> getDocuments(String pipelineName, DatabaseQuery<T> query, int limit, int skip) {
		return connectors.get(pipelineName).getDocumentReader().getDocuments(query, limit, skip);
	}

	private List<DatabaseDocument<T>> discardDocuments(String pipelineName, DatabaseQuery<T> query, int limit, int skip) {
		DatabaseConnector<T> connector = connectors.get(pipelineName);
		List<DatabaseDocument<T>> documents = connector.getDocumentReader().getDocuments(query, limit, skip);
		for (DatabaseDocument<T> document : documents) {
			connector.getDocumentWriter().markDiscarded(document, "admin");
		}
		return documents;
	}	
	
	public Map<String, Object> updateDocuments(String pipelineName, String jsonQuery, int limit,
			String changes) {

		Map<String, Object> ret = new HashMap<String, Object>();
		try {
			DatabaseQuery<AdminServiceType> query = new AdminServiceQuery();
			query.fromJson(jsonQuery);
			List<DatabaseDocument<T>> documents = getDocuments(pipelineName, 
					connectors.get(pipelineName).convert(query), limit, 0);

			if (!documents.isEmpty()) {

				try {
					Map<String, Object> changesMap = SerializationUtils
							.fromJson(changes);
					Set<DatabaseDocument<T>> changedDocuments = new HashSet<DatabaseDocument<T>>();

					@SuppressWarnings("unchecked")
					Map<String, List<String>> deleteObject = (Map<String, List<String>>) changesMap
							.get("deletes");

					if (deleteObject != null) {
						for (DatabaseDocument<T> document : documents) {
							List<String> fetched = deleteObject.get("fetched");
							if (fetched != null) {
								for (String field : fetched) {
									boolean change = document
											.removeFetchedBy(field);
									if (change) {
										connectors.get(pipelineName).getDocumentWriter().update(
												document);
										changedDocuments.add(document);
									}
								}
							}
							List<String> touched = deleteObject.get("touched");
							if (touched != null) {
								for (String field : touched) {
									boolean change = document
											.removeTouchedBy(field);
									if (change) {
										connectors.get(pipelineName).getDocumentWriter().update(
												document);
										changedDocuments.add(document);
									}
								}
							}
						}
					}

					ret.put("numberOfChangedDocuments", changedDocuments.size());
					ret.put("changedDocuments", changedDocuments);
				} catch (ClassCastException e) {
					Map<String, String> error = new HashMap<String, String>();
					error.put("Invalid change map", changes);
					error.put("Expected format:",
							"{\"deletes\":{fetched:[\"staticField\"]},touched:[\"staticField\"]}");
					ret.put("error", error);
					ret.put("numberOfChangedDocuments", 0);
				} catch (JsonException e) {
					Map<String, String> error = new HashMap<String, String>();
					error.put("Invalid change map", changes);
					error.put("Expected format:",
							"{\"deletes\":{fetched:[\"staticField\"]},touched:[\"staticField\"]}");
					ret.put("error", error);
					ret.put("numberOfChangedDocuments", 0);
				}

			}

		} catch (JsonException e) {

			Map<String, String> error = new HashMap<String, String>();
			error.put("Invalid query", jsonQuery);
			ret.put("error", error);
			ret.put("numberOfChangedDocuments", 0);
		}

		return ret;
	}

}
