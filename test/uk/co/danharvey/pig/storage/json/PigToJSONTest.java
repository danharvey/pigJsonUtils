package uk.co.danharvey.pig.storage.json;

import static org.junit.Assert.assertEquals;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Test;

import uk.co.danharvey.pig.storage.json.PigToJSON;

public class PigToJSONTest {

	JSONParser p = new JSONParser();
	
	@SuppressWarnings("unchecked")
	@Test
	public void testTupleToJson_Integer() throws ExecException, ParseException {
		Tuple t = TupleFactory.getInstance().newTuple();
		t.append(1);
		JSONObject json = PigToJSON.tupleToJson(t, (JSONObject)p.parse("{\"fields\": [{\"name\":\"test\"}]}"));
		
		JSONObject realJson = new JSONObject();
		realJson.put("test", 1);
		
		assertEquals(realJson, json);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testTupleToJson_String() throws ExecException, ParseException {
		Tuple t = TupleFactory.getInstance().newTuple();
		t.append("Test");
		JSONObject json = PigToJSON.tupleToJson(t, (JSONObject)p.parse("{\"fields\": [{\"name\":\"test\"}]}"));
		
		JSONObject realJson = new JSONObject();
		realJson.put("test", "Test");
		
		assertEquals(realJson, json);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testTupleToJson_Bag() throws ExecException, ParseException {
		Tuple t = TupleFactory.getInstance().newTuple();
		DataBag b = BagFactory.getInstance().newDefaultBag();
		
		Tuple t1 = TupleFactory.getInstance().newTuple();
		t1.append("test0");
		b.add(t1);
		
		Tuple t2 = TupleFactory.getInstance().newTuple();
		t2.append("test1");
		b.add(t2);
		
		t.append(b);
		
		JSONObject json = PigToJSON.tupleToJson(t, (JSONObject)p.parse("{\"fields\": [{\"name\":\"test_bag\", \"fields\": [{\"name\":\"field1\"}]}]}"));
		
		JSONObject realJson = new JSONObject();
		JSONArray array = new JSONArray();
		
		JSONObject j1 = new JSONObject();
		j1.put("field1", "test0");
		
		JSONObject j2 = new JSONObject();
		j2.put("field1", "test1");
		
		array.add(j1);
		array.add(j2);
		
		realJson.put("test_bag", array);
		
		assertEquals(realJson, json);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testTupleToJson_Tuple() throws ExecException, ParseException {
		Tuple t = TupleFactory.getInstance().newTuple();
		
		Tuple t1 = TupleFactory.getInstance().newTuple();
		t1.append("test0");
		t1.append("test1");
		
		t.append(t1);
		
		JSONObject json = PigToJSON.tupleToJson(t, (JSONObject)p.parse("{\"fields\": [{\"name\":\"test_tuple\", \"fields\": [{\"name\":\"field1\"}, {\"name\":\"field2\"}]}]}"));
		
		JSONObject realJson = new JSONObject();
		
		JSONObject j1 = new JSONObject();
		j1.put("field1", "test0");
		j1.put("field2", "test1");
		
		realJson.put("test_tuple", j1);
		
		assertEquals(realJson, json);
	}
}
