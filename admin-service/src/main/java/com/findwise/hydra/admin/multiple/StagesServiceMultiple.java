package com.findwise.hydra.admin.multiple;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.Pipeline;
import com.findwise.hydra.Stage;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.SerializationUtils;

public class StagesServiceMultiple<T extends DatabaseType> {

	private Map<String, DatabaseConnector<T>> connectors;
	private static Logger logger = LoggerFactory
			.getLogger(StagesServiceMultiple.class);
	
	public StagesServiceMultiple(Map<String, DatabaseConnector<T>> connectors) {
		this.connectors = connectors;
		for (DatabaseConnector<T> connector : connectors.values()) {
			try {
				connector.connect();
			} catch (IOException e) {
				logger.error("Failed to connect");
			}
		}
	}

	public Map<String, List<Stage>> getStages(String pipelineName) {
		Map<String, List<Stage>> ret = new HashMap<String, List<Stage>>();
		DatabaseConnector<T> connector = connectors.get(pipelineName);
		List<Stage> stages = connector.getPipelineReader().getPipeline().getStages();
		ret.put("stages", stages);

		return ret;
	}

	public Map<String, Object> addStage(String pipelineName, String libraryId,
			String name, String jsonConfig) throws JsonException, IOException {
		Map<String, Object> ret = new HashMap<String, Object>();
		addStage(pipelineName, libraryId, name, jsonConfig, false);
		ret.put("stageStatus", "Added");
		return ret;
	}

	public void addStage(String pipelineName, String libraryId, String name,
			String jsonConfig, boolean debug) throws JsonException, IOException {
		DatabaseFile df = new DatabaseFile();
		DatabaseConnector<T> connector = connectors.get(pipelineName);
		
		try {
			df.setId(new ObjectId(libraryId));
		} catch (Exception e) {
			df.setId(libraryId);
		}
		Stage s = new Stage(name, df);
		s.setProperties(SerializationUtils.fromJson(jsonConfig));
		if (debug) {
			s.setMode(Stage.Mode.DEBUG);
		} else {
			s.setMode(Stage.Mode.ACTIVE);
		}

		Pipeline<Stage> pipeline = connector.getPipelineReader().getPipeline();
		pipeline.addStage(s);
		connector.getPipelineWriter().write(pipeline);

	}

	public Stage getStageInfo(String pipelineName, String stageName) {
		return connectors.get(pipelineName).getPipelineReader().getPipeline()
				.getStage(stageName);
	}

}
