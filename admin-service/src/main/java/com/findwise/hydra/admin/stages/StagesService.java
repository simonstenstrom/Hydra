package com.findwise.hydra.admin.stages;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.Pipeline;
import com.findwise.hydra.SerializationUtils;
import com.findwise.hydra.Stage;
import com.findwise.hydra.StageGroup;

public class StagesService<T extends DatabaseType> {

	private DatabaseConnector<T> connector;

	public StagesService(DatabaseConnector<T> connector) {
		this.connector = connector;
	}

	public DatabaseConnector<T> getConnector() {
		return connector;
	}
	
	public Map<String, Set<Stage>> getStages() {
		Map<String, Set<Stage>> ret = new HashMap<String, Set<Stage>>();
		ret.put("stages", connector.getPipelineReader().getPipeline().getStages());
		return ret;
	}

	public Map<String, Object> addStage(String libraryId, String name,
			String jsonConfig) throws JsonException, IOException {
		Map<String, Object> ret = new HashMap<String, Object>();
		addStage(libraryId, name, jsonConfig, false);
		ret.put("stageStatus", "Added");
		return ret;
	}

	public void addStage(String libraryId, String name, String jsonConfig,
			boolean debug) throws JsonException, IOException {
		DatabaseFile df = new DatabaseFile();
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

		Pipeline pipeline = connector.getPipelineReader().getPipeline();
		StageGroup sg = new StageGroup(s.getName());
		sg.addStage(s);
		pipeline.addGroup(sg);
		connector.getPipelineWriter().write(pipeline);

	}

	public Stage getStageInfo(String stageName) {
		return connector.getPipelineReader().getPipeline().getStage(stageName);
	}

}
