package com.findwise.hydra.admin.documents;

import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.google.gson.Gson;

public class TestDocumentsService {

	@Test
	public void testChangesFormat() {
		String changes = "{ 'deletes' : 'test', 'blubb' : 'tjo' }";
		
		Gson gson = new Gson();
		Map changesMap = gson.fromJson(changes, Map.class);
		
		System.out.println(changesMap.getClass().getName());
		
		//Assert.assertNotNull(changesMap.get("deletes"));
	}

}
