package com.blogspot.sahyog.json;

import com.fasterxml.jackson.databind.node.ObjectNode;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Stack;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonFactory;
import java.io.File;

/**
 * The objective of this class is to convert a file which has an array of json
 * objects into another file with an array of json objects where each object in
 * the array is on its own line. This allows for this file to be split along
 * line boundaries for processing by splittable input file consumers like hadoop
 *
 * @author puneet
 *
 */
public class JsonArraySingleLineObjects {

    public static void main(String args[]) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: JsonArraySingleLineObjects ipFile opFile");
            System.exit(1);
        }
        File ipFile = new File(args[0]);
        if (!ipFile.exists()) {
            System.err.println("File:" + ipFile.getAbsolutePath() + " does not exist");
            System.exit(1);
        }
        File opFile = new File(args[1]);
        JsonFactory factory = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(factory);
        JsonParser jp = null;
        JsonGenerator jgen = null;
        int numRecords = 0;
        try {
            jp = factory.createParser(ipFile);
            jgen = factory.createGenerator(opFile, JsonEncoding.UTF8);
            if (jp.nextToken() != JsonToken.START_ARRAY) {
                System.err.println("Improper format. The file should have just a json array of objects");
                System.exit(1);
            }
            jgen.writeStartArray();
            jgen.writeRaw(System.getProperty("line.separator"));
            while (true) {
                JsonToken token = jp.nextToken();
                if (token == JsonToken.END_ARRAY) {
                    break;
                }
                if (token != JsonToken.START_OBJECT) {
                    System.err.println("Improper format. The file should have just a json array of objects");
                    System.exit(1);
                }
                ObjectNode node = mapper.readTree(jp);
                jgen.writeTree(node);
                jgen.writeRaw(System.getProperty("line.separator"));
                numRecords++;
            }
            jgen.writeEndArray();
            System.out.println("Wrote: " + numRecords);
        } finally {
            if (jp != null) {
                jp.close();
            }
            if (jgen != null) {
                jgen.close();
            }
        }

    }
}
