package com.findwise.hydra.admin.documents;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.admin.database.AdminServiceQuery;
import com.findwise.hydra.admin.database.AdminServiceType;
import com.findwise.hydra.common.JsonException;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

public class DocumentsService<T extends DatabaseType> {

	private DatabaseConnector<T> connector;

	public DocumentsService(DatabaseConnector<T> connector) {
		this.connector = connector;
	}

	public Map<String, Object> getNumberOfDocuments(String jsonQuery) {
		Map<String, Object> ret = new HashMap<String, Object>();

		try {
			DatabaseQuery<AdminServiceType> query = new AdminServiceQuery();
			query.fromJson(jsonQuery);
			ret.put("numberOfDocuments",
					getNumberOfDocuments(connector.convert(query)));
		} catch (JsonException e) {
			Map<String, String> error = new HashMap<String, String>();
			error.put("Invalid query", jsonQuery);
			ret.put("error", error);
			ret.put("numberOfDocuments", 0);
		}

		return ret;
	}

	public Map<String, Object> getDocuments(String jsonQuery, int limit,
			int skip) {

		Map<String, Object> ret = new HashMap<String, Object>();
		try {
			DatabaseQuery<AdminServiceType> query = new AdminServiceQuery();
			query.fromJson(jsonQuery);
			ret.put("documents",
					getDocuments(connector.convert(query), limit, skip));
		} catch (JsonException e) {
			Map<String, String> error = new HashMap<String, String>();
			error.put("Invalid query", jsonQuery);
			ret.put("error", error);
			ret.put("documents", new ArrayList<DatabaseDocument<T>>());
		}

		return ret;
	}

	private long getNumberOfDocuments(DatabaseQuery<T> query) {
		return getConnector().getDocumentReader().getNumberOfDocuments(
				(DatabaseQuery<T>) query);
	}

	private List<DatabaseDocument<T>> getDocuments(DatabaseQuery<T> query,
			int limit, int skip) {
		return getConnector().getDocumentReader().getDocuments(query, limit,
				skip);
	}

	public DatabaseConnector<T> getConnector() {
		return connector;
	}

	public Map<String, Object> updateDocuments(String jsonQuery, int limit,
			String changes) {

		Map<String, Object> ret = new HashMap<String, Object>();
		try {
			DatabaseQuery<AdminServiceType> query = new AdminServiceQuery();
			query.fromJson(jsonQuery);
			List<DatabaseDocument<T>> documents = getDocuments(
					connector.convert(query), limit, 0);

			if (!documents.isEmpty()) {
				
				try {
					Gson gson = new Gson();
					Map<?,?> changesMap = gson.fromJson(changes, Map.class);
					Set<DatabaseDocument<T>> changedDocuments = new HashSet<DatabaseDocument<T>>();
					
					Object deleteObject = changesMap.get("deletes");
					List<?> deletes = (List<?>) deleteObject;
					for (DatabaseDocument<T> document : documents) {
						for (Object field : deletes) {
							boolean change = document.removeMetadataField((String) field);
							if (change) {
								changedDocuments.add(document);
							}
						}
					}

					Object addObject = changesMap.get("adds");
					Map<?, ?> adds = (Map<?, ?>) addObject;
					for (DatabaseDocument<T> document : documents) {
						for (Entry<?, ?> field : adds.entrySet()) {
							Object change = document.putMetadataField((String) field.getKey(), field.getValue());

							if (change != null) {
								changedDocuments.add(document);
							}
						}
					}

					ret.put("numberOfChangedDocuments", changedDocuments.size());
					ret.put("changedDocuments", changedDocuments);
				} catch (ClassCastException e) {
					Map<String, String> error = new HashMap<String, String>();
					error.put("Invalid change map", changes);
					error.put("Expected format:",
							"{deletes:[fetched.staticFieldStage,touched.staticFieldStage],adds:{newmetadatakey:newmetadatavalue}}");
					ret.put("error", error);
					ret.put("numberOfChangedDocuments", 0);
				} catch (JsonParseException e) {
					Map<String, String> error = new HashMap<String, String>();
					error.put("Invalid change map", changes);
					error.put("Expected format:",
							"{deletes:[fetched.staticFieldStage,touched.staticFieldStage],adds:{newmetadatakey:newmetadatavalue}}");
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
