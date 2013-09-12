package com.findwise.hydra.admin.documents;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.findwise.hydra.SerializationUtils;

public class TestJsonMap {
	@SuppressWarnings("unchecked")
	@Test
	public void testMap() throws Exception {
		Map<String, Object> x = new HashMap<String, Object>();

		x.put("deletes", Arrays.asList("metadata.fetched.staticField", "metadata.touched.staticField"));
		String json = SerializationUtils.toJson(x);
		System.out.println(json);
		Map<String, Object> y = SerializationUtils.fromJson(json);

		Assert.assertEquals(x.get("deletes"), y.get("deletes"));

		Map<String, String> adds = new HashMap<String, String>();
		adds.put("metadata.fetched.staticField", "1234");

		x.put("adds", adds);

		json = SerializationUtils.toJson(x);
		System.out.println(json);
		y = SerializationUtils.fromJson(json);
		Assert.assertEquals(((Map<String, String>) x.get("adds")).get("metadata.fetched.staticField"),
				((Map<String, String>) y.get("adds")).get("metadata.fetched.staticField"));
	}
}
