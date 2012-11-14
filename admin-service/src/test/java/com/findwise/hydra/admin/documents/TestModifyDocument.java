package com.findwise.hydra.admin.documents;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.mongodb.MongoDocument;

public class TestModifyDocument {
	
	private MongoDocument mongoDocument;

	@Before
	public void init() throws Exception {
		mongoDocument = new MongoDocument();
		Map<String,Object> fetchMap = new HashMap<String, Object>();
		fetchMap.put("StaticField", 465232);
		fetchMap.put("Rename", 879124);
		mongoDocument.putMetadataField("fetched", fetchMap);
		
		Map<String,Object> touch = new HashMap<String, Object>();
		touch.put("StaticField", 465236);
		touch.put("Rename", 879127);
		mongoDocument.putMetadataField("touched", touch);
		
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void testChangeMetadata() throws Exception {
		Map<String,Object> fetched = (Map<String,Object>)mongoDocument.getMetadataMap().get("fetched");
		Map<String,Object> touched = (Map<String,Object>)mongoDocument.getMetadataMap().get("touched");
		Assert.assertEquals(2, fetched.size());
		Assert.assertEquals(2, touched.size());
		mongoDocument.removeFetchedBy("StaticField");
		fetched = (Map<String,Object>)mongoDocument.getMetadataMap().get("fetched");
		touched = (Map<String,Object>)mongoDocument.getMetadataMap().get("touched");
		Assert.assertEquals(1, fetched.size());
		Assert.assertEquals(2, touched.size());
		
	}

}
