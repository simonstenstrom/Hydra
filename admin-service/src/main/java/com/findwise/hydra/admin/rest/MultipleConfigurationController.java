package com.findwise.hydra.admin.rest;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.multipart.MultipartFile;

import com.findwise.hydra.JsonException;
import com.findwise.hydra.Stage;
import com.findwise.hydra.admin.multiple.ConfigurationServiceMultiple;
import com.findwise.hydra.admin.multiple.DocumentsServiceMultiple;
import com.findwise.hydra.admin.multiple.StagesServiceMultiple;

@Controller("/rest")
public class MultipleConfigurationController {

	@Autowired
	private ConfigurationServiceMultiple<?> configurationService;

	@Autowired
	private DocumentsServiceMultiple<?> documentService;

	@Autowired
	private StagesServiceMultiple<?> stagesService;

	public void setDocumentService(DocumentsServiceMultiple<?> documentService) {
		this.documentService = documentService;
	}

	public void setService(ConfigurationServiceMultiple<?> service) {
		this.configurationService = service;
	}

	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/pipelines/{pipelineName}")
	public Map<String, Object> getStats(@PathVariable String pipelineName) {
		return configurationService.getStats(pipelineName);
	}

	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/pipelines/{pipelineName}/libraries")
	public Map<String, Object> getLibraries(@PathVariable String pipelineName) {
		return configurationService.getLibraries(pipelineName);
	}

	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/pipelines/{pipelineName}/libraries/{id}")
	public Map<String, Object> getLibrary(@PathVariable String pipelineName, @PathVariable String id) {
		return configurationService.getLibrary(pipelineName, id);
	}

	@ResponseStatus(HttpStatus.ACCEPTED)
	@RequestMapping(method = RequestMethod.POST, value = "/pipelines/{pipelineName}/libraries/{id}")
	@ResponseBody
	public Map<String, Object> addLibrary(@PathVariable String pipelineName, @PathVariable String id, @RequestParam MultipartFile file)
			throws IOException {
		configurationService.addLibrary(pipelineName, id, file.getOriginalFilename(), file.getInputStream());
		return getLibrary(pipelineName, id);
	}

	@ResponseStatus(HttpStatus.ACCEPTED)
	@RequestMapping(method = RequestMethod.POST, value = "/pipelines/{pipelineName}/libraries/{id}/stages/{stageName}")
	@ResponseBody
	public Map<String, Object> addStage(@PathVariable String pipelineName, @PathVariable(value = "id") String libraryId,
			@PathVariable(value = "stageName") String stageName, @RequestBody String jsonConfig) throws JsonException, IOException {
		return stagesService.addStage(pipelineName, libraryId, stageName, jsonConfig);
	}

	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/pipelines/{pipelineName}/stages")
	public Map<String, Set<Stage>> getStages(@PathVariable String pipelineName) {
		return stagesService.getStages(pipelineName);
	}

	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/pipelines/{pipelineName}/stages/{stageName}")
	public Stage getStageInfo(@PathVariable String pipelineName, @PathVariable(value = "stageName") String stageName) {
		return stagesService.getStageInfo(pipelineName, stageName);
	}

	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/pipelines/{pipelineName}/documents/count")
	public Map<String, Object> getDocumentCount(@PathVariable String pipelineName,
			@RequestParam(required = false, defaultValue = "{}", value = "q") String jsonQuery) {
		return documentService.getNumberOfDocuments(pipelineName, jsonQuery);
	}

	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/pipelines/{pipelineName}/documents")
	public Map<String, Object> getDocuments(@PathVariable String pipelineName,
			@RequestParam(required = false, defaultValue = "{}", value = "q") String jsonQuery,
			@RequestParam(required = false, defaultValue = "10", value = "limit") int limit,
			@RequestParam(required = false, defaultValue = "0", value = "skip") int skip) {
		return documentService.getDocuments(pipelineName, jsonQuery, limit, skip);
	}

	@ResponseBody
	@RequestMapping(method = RequestMethod.POST, value = "/pipelines/{pipelineName}/documents/edit")
	public Map<String, Object> editDocuments(@PathVariable String pipelineName,
			@RequestParam(required = false, defaultValue = "{}", value = "q") String jsonQuery,
			@RequestParam(required = false, defaultValue = "1", value = "limit") int limit, @RequestBody String changes) {
		return documentService.updateDocuments(pipelineName, jsonQuery, limit, changes);
	}

	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/pipelines/{pipelineName}/documents/discard")
	public Map<String, Object> discardDocuments(@PathVariable String pipelineName,
			@RequestParam(required = false, defaultValue = "{}", value = "q") String jsonQuery,
			@RequestParam(required = false, defaultValue = "1", value = "limit") int limit,
			@RequestParam(required = false, defaultValue = "0", value = "skip") int skip) {
		return documentService.discardDocuments(pipelineName, jsonQuery, limit, skip);
	}
}
