package uk.co.danharvey.pig.storage.json;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class PigToJSON {

	/**
	 * Convert a Pig Tuple into a JSON object
	 */
	@SuppressWarnings("unchecked")
	protected static JSONObject tupleToJson(Tuple t, JSONObject schema) throws ExecException {
		JSONObject json = new JSONObject();
		JSONArray fieldSchemas = (JSONArray)schema.get("fields");
		for (int i=0; i<t.size(); i++) {
			Object field = t.get(i);
			JSONObject fieldSchema = (JSONObject)fieldSchemas.get(i);
			json.put(fieldSchema.get("name"), fieldToJson(field, fieldSchema));
		}
		return json;
	}

	/**
	 * Convert a Pig Bag to a JSON array 
	 */
	@SuppressWarnings("unchecked")
	private static JSONArray bagToJson(DataBag bag, JSONObject schema) throws ExecException {
		JSONArray array = new JSONArray();
		for (Tuple t: bag) {
			JSONObject recJson = tupleToJson(t, schema);
			array.add(recJson);
		}
		return array;
	}

	/**
	 * Find the type of a field and convert it to JSON as required.
	 */
	private static Object fieldToJson(Object field, JSONObject schema) throws ExecException {
		switch (DataType.findType(field)) {
				// Native types that don't need converting
				case DataType.NULL:
				case DataType.INTEGER:
				case DataType.BOOLEAN:
				case DataType.LONG:
				case DataType.FLOAT:
				case DataType.DOUBLE:
				case DataType.CHARARRAY:
					return field;
				
				case DataType.TUPLE:
					return tupleToJson((Tuple)field, schema);
				
				case DataType.BAG:
					return bagToJson(DataType.toBag(field), schema);
					
				case DataType.MAP:
					throw new ExecException("Map type is not current supported with JsonStorage");
				
				case DataType.BYTE:
					throw new ExecException("Byte type is not current supported with JsonStorage");
					
				case DataType.BYTEARRAY:
					throw new ExecException("ByteArray type is not current supported with JsonStorage");
				
				default:
					throw new ExecException("Unknown type is not current supported with JsonStorage");
		}
	}

	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	protected static JSONObject convertSchemaToJson(ResourceSchema schema) {
		JSONObject jsonSchema = new JSONObject();
		JSONArray fields = new JSONArray();
		
		for (ResourceFieldSchema field: schema.getFields()) {
			JSONObject jsonField = new JSONObject();
			jsonField.put("name", field.getName());
			
			switch (field.getType()) {
				case DataType.INTEGER:
				case DataType.DOUBLE:
				case DataType.FLOAT:
					jsonField.put("type", "number");
					break;
				
				case DataType.CHARARRAY:
					jsonField.put("type", "string");
					break;
					
				case DataType.BOOLEAN:
					jsonField.put("type", "boolean");
					break;
				
				case DataType.TUPLE:
					jsonField.put("type", "object");
					break;
					
				case DataType.BAG:
					jsonField.put("type", "array");
					break;
					
				case DataType.BYTE:
			}
			
			fields.add(jsonField);
		}
		
		jsonSchema.put("fields", fields);
		return jsonSchema;
	}

}
