package uk.co.danharvey.pig.storage.json;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class JsonStorage extends StoreFunc {
	private RecordWriter<Text, NullWritable> writer;
	private JSONObject schema = null;
	
	@Override
	public OutputFormat<Text, NullWritable> getOutputFormat() throws IOException {
		return new TextOutputFormat<Text, NullWritable>();
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer = writer;
	}

	@Override
	public void putNext(Tuple tuple) throws IOException {
		JSONObject json = PigToJSON.tupleToJson(tuple, schema);
		try {
			writer.write(new Text(json.toJSONString()), NullWritable.get());
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
	
	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
	    job.getConfiguration().set("mapred.textoutputformat.separator", "");
        FileOutputFormat.setOutputPath(job, new Path(location));
        if (location.endsWith(".bz2")) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job,  BZip2Codec.class);
        }  else if (location.endsWith(".gz")) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        }
        
        // Load or save schema
        if (schema == null) {
        	schema = loadSchema(location, job);
        } else {
        	saveSchema(location, job);
        }
	}
	
	private void saveSchema(String location, Job job) throws IOException {
    	FileSystem fs = FileSystem.get(job.getConfiguration());
    	DataOutputStream os = fs.create(new Path(location).suffix("_schema.json"));
    	os.writeUTF(schema.toJSONString());
    	os.close();
    	fs.close();
	}

	private static JSONObject loadSchema(String location, Job job) throws IOException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
    	DataInputStream is = fs.open(new Path(location).suffix("_schema.json"));
    	String json = is.readUTF();
    	is.close();
    	fs.close();
    	
    	try {
			return (JSONObject)new JSONParser().parse(json);
		} catch (ParseException e) {
			throw new IOException("Could not parse JSON schema", e);
		} 
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {
		// Serialise into JSON to serialise into HDFS
		this.schema = PigToJSON.convertSchemaToJson(s);		
	}
}
