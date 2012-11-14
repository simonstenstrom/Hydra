package com.findwise.hydra.admin.multiple;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.Pipeline;
import com.findwise.hydra.Stage;
import com.findwise.hydra.admin.PipelineScanner;
import com.findwise.hydra.admin.StageInformation;

public class ConfigurationServiceMultiple<T extends DatabaseType> {
	private Map<String, DatabaseConnector<T>> connectors;
	
	private static final String TMP_DIR = "tmp";
	private static Logger logger = LoggerFactory
			.getLogger(ConfigurationServiceMultiple.class);

	public ConfigurationServiceMultiple(Map<String, DatabaseConnector<T>> connectors) {
		this.connectors = connectors;
		for (DatabaseConnector<T> connector : connectors.values()) {
			try {
				connector.connect();
			} catch (IOException e) {
				logger.error("Failed to connect");
			}
		}
	}


	public void addLibrary(String pipelineName, String id, String filename, InputStream stream) {
		connectors.get(pipelineName).getPipelineWriter().save(id, filename, stream);
	}

	public Map<String, Object> getLibraries(String pipelineName) {
		Map<String, Object> map = new HashMap<String, Object>();

		PipelineScanner<T> pipelineScanner = new PipelineScanner<T>(connectors.get(pipelineName).getPipelineReader());
		
		for (DatabaseFile df : pipelineScanner.getLibraryFiles()) {
			map.put(df.getId().toString(), getLibraryMap(pipelineName, df));
		}

		return map;
	}
	
	public Map<String, Object> getLibrary(String pipelineName, String id) {
		PipelineScanner<T> pipelineScanner = new PipelineScanner<T>(connectors.get(pipelineName).getPipelineReader());
		for (DatabaseFile df : pipelineScanner.getLibraryFiles()) {
			if(df.getId().toString().equals(id)) {
				return getLibraryMap(pipelineName, df);
			}
		}
		return null;
	}

	private Map<String, Object> getLibraryMap(String pipelineName, DatabaseFile df) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("filename", df.getFilename());
		map.put("uploaded", df.getUploadDate());
		map.put("stages", getStagesMap(pipelineName, df));
		return map;
	}

	private Map<String, Object> getStagesMap(String pipelineName, DatabaseFile df) {
		Map<String, Object> map = new HashMap<String, Object>();
		PipelineScanner<T> pipelineScanner = new PipelineScanner<T>(connectors.get(pipelineName).getPipelineReader());
		try {
			for (Class<?> c : pipelineScanner.getStageClasses(new File(TMP_DIR), df)) {
				try {
					map.put(c.getCanonicalName(), new StageInformation(c));
				} catch (NoSuchElementException e) {
					logger.error("Unable to get stage information for class "+c.getCanonicalName(), e);
				}
			}
		} catch (IOException e) {
			logger.error("Unable to get stage classes", e);
		}
		return map;
	}

	public Map<String, Object> getStats(String pipelineName) {
		Map<String, Object> map = new HashMap<String, Object>();
		Map<String, Object> documentMap = new HashMap<String, Object>();

		documentMap.put("current", connectors.get(pipelineName).getDocumentReader()
				.getActiveDatabaseSize());
		documentMap.put("throughput", 0);
		documentMap.put("archived", connectors.get(pipelineName).getDocumentReader()
				.getInactiveDatabaseSize());
		documentMap.put("status", new HashMap<String, Long>());

		map.put("documents", documentMap);

		Map<String, Object> stageMap = new HashMap<String, Object>();
		map.put("stages", stageMap);

		stageMap.put("active", getStageConfigMap(connectors.get(pipelineName).getPipelineReader()
				.getPipeline()));

		stageMap.put("debug", getStageConfigMap(connectors.get(pipelineName).getPipelineReader()
				.getDebugPipeline()));

		return map;
	}

	private Map<String, Object> getStageConfigMap(Pipeline<Stage> pipeline) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (Stage s : pipeline.getStages()) {
			HashMap<String, Object> stage = new HashMap<String, Object>();
			stage.put("properties", s.getProperties());

			HashMap<String, Object> file = new HashMap<String, Object>();
			file.put("id", s.getDatabaseFile().getId());
			file.put("name", s.getDatabaseFile().getFilename());
			stage.put("file", file);
			map.put(s.getName(), stage);
		}
		return map;
	}
}
