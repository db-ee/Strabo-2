package sesameWrapper;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.Json;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class LivyHelper {

    protected static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    protected static final List<String> udfs = Collections.unmodifiableList(Arrays.asList("ST_PointFromText", "ST_PolygonFromText", "ST_LineStringFromText", "ST_GeomFromText", "ST_GeomFromWKT", "ST_GeomFromWKB", "ST_GeomFromGeoJSON", "ST_Point", "ST_PolygonFromEnvelope", "ST_Contains", "ST_Intersects", "ST_Within", "ST_Distance", "ST_ConvexHull", "ST_NPoints", "ST_Buffer", "ST_Envelope", "ST_Length", "ST_Area", "ST_Centroid", "ST_Transform", "ST_Intersection", "ST_IsValid", "ST_PrecisionReduce", "ST_Equals", "ST_Touches", "ST_Overlaps", "ST_Crosses", "ST_IsSimple", "ST_MakeValid", "ST_SimplifyPreserveTopology", "ST_AsText", "ST_GeometryType", "ST_NumGeometries", "ST_LineMerge", "ST_Azimuth", "ST_X", "ST_Y", "ST_StartPoint", "ST_Boundary", "ST_EndPoint", "ST_ExteriorRing", "ST_GeometryN", "ST_InteriorRingN", "ST_Dump", "ST_DumpPoints", "ST_IsClosed", "ST_NumInteriorRings", "ST_AddPoint", "ST_RemovePoint", "ST_IsRing"));
    protected static final List<String> aggregateUdfs = Collections.unmodifiableList(Arrays.asList("ST_Union_Aggr", "ST_Envelope_Aggr", "ST_Intersection_Aggr"));
    //private ArrayList<String> outputs;
    //private Reader result = null;
    //private Response response2;
    private static final Logger log = LoggerFactory.getLogger(LivyHelper.class);
    //private OkHttpClient client = new OkHttpClient();


    private static String post(String url, String json, OkHttpClient client) throws IOException {
        RequestBody body = RequestBody.create(json, JSON);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            //return response.body().string();
            return response.body().string();
        }
    }

    protected static String createSession() throws IOException {

	OkHttpClient client = new OkHttpClient();

        String createResponse = post(ConnectionConstants.LIVYURL, create, client);

        //System.out.println(createResponse.files.values());
        int id = JsonPath.parse(createResponse).read("id");

        String sessionURL = ConnectionConstants.LIVYURL + "/" + id;

        Request request = new Request.Builder()
                .url(sessionURL)
                .build();
        String status = "";
        while (true) {
            try (Response response = client.newCall(request).execute()) {
                status = JsonPath.parse(response.body().string()).read("state");
                System.out.println("status: " + status);
                if (status.equals("idle"))
                    break;
                if (!status.equals("starting")) {
                    System.out.println("Could not start session. Status: " + status + " exiting...");
                    return null;
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        return sessionURL;
    }

    protected static void closeSession(String sessionUrl) throws IOException {
        RequestBody body = RequestBody.create("", JSON);
        Request request = new Request.Builder()
                .url(sessionUrl)
                .delete()
                .build();
        OkHttpClient client = new OkHttpClient();
        try (Response response = client.newCall(request).execute()) {
            //return response.body().string();
            System.out.println("Deleting session:" + response.body().string());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected static String getSQLQuery(String query) {
	//return "{ \"code\": \"val d = spark.sql(\\\"" + query + "\\\").toJSON.toLocalIterator\\n while (d.hasNext) { println(d.next)}\"}";
	//return "{ \"code\": \"val d = spark.sql(\\\"" + query + "\\\")randomSplit(Array(0.1, 0.5, 0.5, 0.5, 0.5, 0.5))\\nd(0).toJSON.collect\\n%json\"}";
        return "{ \"code\": \"val d = spark.sql(\\\"" + query + "\\\").toJSON.collect\\n%json\"}";
	//return "{ \"code\": \"val d = spark.sql(\\\"" + query + "\\\").collect\"}";
	//return "{ \"code\": \"println(spark.sql(\\\"" + query + "\\\").collect.mkString(\\\"[\\\", \\\"\\\\n\\\", \\\"]\\\"))\"}";
        //return "{ \"code\": \"println(spark.sql(\\\"" + query + "\\\").toJSON.collect.mkString(\\\"[\\\", \\\"\\\\n\\\", \\\"]\\\"))\"}";
    }

   protected static String getSQLQuerySplit(String query, int splitNumber) {
	String splitString = "1.0";
	for (int i=1;i<splitNumber;i++) {
		splitString += ", 1.0";
	}
        return "{ \"code\": \"val d1 = spark.sql(\\\"" + query + "\\\").cache()\\nval d =d1.randomSplit(Array(" + splitString +"))\"}";
        
    }

    protected static String getSQLQueryPart(int part) {
        return "{ \"code\": \"d("+part+").toJSON.collect\\n%json\"}";


    }

    protected static String getSQLWriteResult(String query, String filename) {
	return "{ \"code\": \"val d = spark.sql(\\\"" + query + "\\\").write.csv(\\\""+filename+"\\\")\"}";
    }

  /*  private static String proxyUser = "test__meb10000";
    private static int numExecutors = 1;
    private static int executorCores = 2;
    private static String executorMemory = "5G";
    private static String sparkDriverExtraClassPath = "{{PWD}}:/srv/hops/spark/jars/*:/srv/hops/spark/hopsworks-jars/*";
    private static String sparkExecutorExtraClassPath = "{{PWD}}:/srv/hops/spark/jars/*:/srv/hops/spark/hopsworks-jars/*";
    private static String appMasterEnvSparkHome = "/srv/hops/spark";
    private static String executorEnvSparkHome = "/srv/hops/spark";
    private static String appMasterEnvSparkConfDir = "/srv/hops/spark/conf";
    private static String executorEnvSparkConfDir = "/srv/hops/spark/conf";
    private static String appMasterEnvHadoopHome = "/srv/hops/hadoop";
    private static String executorEnvHadoopHome = "/srv/hops/hadoop";
    private static String appMasterEnvHdfsHome = "/srv/hops/hadoop";
    private static String executorEnvHdfsHome = "/srv/hops/hadoop";
    private static String appMasterEnvHadoopUserName = "test__meb10000";
    private static String executorEnvHadoopUserName = "test__meb10000";
    private static String sparkYarnDistFiles = "hdfs:///user/spark/metrics.properties,hdfs:///user/spark/log4j.properties,hdfs:///user/spark/hive-site.xml";
    private static String sparkYarnDistJars = "local:///srv/hops/apache-livy/rsc-jars/livy-api.jar,local:///srv/hops/apache-livy/rsc-jars/livy-rsc.jar," +
            "local:///srv/hops/apache-livy/rsc-jars/netty-all.jar,local:///srv/hops/apache-livy/repl_2.11-jars/commons-codec.jar," +
            "local:///srv/hops/apache-livy/repl_2.11-jars/livy-core.jar,local:///srv/hops/apache-livy/repl_2.11-jars/livy-repl.jar," +
            "local:///srv/hops/spark/jars/datanucleus-api.jar,local:///srv/hops/spark/jars/datanucleus-core.jar";
    private static String livyRpcServerAddress = "10.0.10.50";
    private static String sparkYarnStagingDir = "hdfs:///Projects/test/Resources";
    private static String sparkJars = "local:///tmp/geospark-1.3.2.jar,local:///tmp/geospark-sql_2.3-1.3.2.jar,local:///tmp/geospark-viz_2.3-1.3.1.jar";
    private static String sparkSerializer = "org.apache.spark.serializer.KryoSerializer";
    private static String sparkKryoRegistrator = "org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator";
    //private static String sparkJarsIvy = "/tmp/geospark*";
    private static String sparkExtraListeners = "org.apache.spark.sql.geosparksql.UDT.GeoSparkRegistratorListener";

    protected static String livyUrl = "http://10.0.10.50:8998/sessions";
    protected static String databaseName = "test";
*/
    protected static String create = "{\"kind\": \"spark\", \"proxyUser\": \""+ ConnectionConstants.PROXYUSER+"\"," +
            " \"numExecutors\": "+ConnectionConstants.NUMEXECUTORS+", \"executorCores\": "+ConnectionConstants.EXECUTORCORES+ ", \"driverCores\": "+ConnectionConstants.DRIVERCORES+ ", \"executorMemory\": \""+ConnectionConstants.EXECUTORMEMORY+"\", \"driverMemory\": \""+ConnectionConstants.DRIVERMEMORY+"\", \"conf\" : {\"spark.submit.deployMode\": \"cluster\"," +
            " \"spark.driver.extraClassPath\": \""+ConnectionConstants.SPARKDRIVEREXTRACLASSPATH+"\"," +
            " \"spark.executor.extraClassPath\": \""+ConnectionConstants.SPARKEXECUTOREXTRACLASSPATH+"\"," +
            " \"spark.yarn.appMasterEnv.SPARK_HOME\":\""+ConnectionConstants.APPMASTERENVHADOOPHOME+"\", \"spark.executorEnv.SPARK_HOME\":\""+ConnectionConstants.EXECUTORENVSPARKHOME+"\"," +
            " \"spark.yarn.appMasterEnv.SPARK_CONF_DIR\":\""+ConnectionConstants.APPMASTERENVSPARKCONFDIR+"\", \"spark.executorEnv.SPARK_CONF_DIR\":\""+ConnectionConstants.EXECUTORENVSPARKCONFDIR+"\"," +
            " \"spark.yarn.appMasterEnv.HADOOP_HOME\": \""+ConnectionConstants.APPMASTERENVHADOOPHOME+"\", \"spark.executorEnv.HADOOP_HOME\": \""+ConnectionConstants.EXECUTORENVHADOOPHOME+"\"," +
            " \"spark.yarn.appMasterEnv.HADOOP_HDFS_HOME\": \""+ConnectionConstants.APPMASTERENVHDFSHOME+"\", \"spark.executorEnv.HADOOP_HDFS_HOME\": \""+ConnectionConstants.EXECUTORENVHDFSHOME+"\"," +
            " \"spark.yarn.appMasterEnv.HADOOP_USER_NAME\":\""+ConnectionConstants.APPMASTERENVHADOOPUSERNAME+"\", \"spark.executorEnv.HADOOP_USER_NAME\":\""+ConnectionConstants.EXECUTORENVHADOOPUSERNAME+"\"," +
            " \"spark.yarn.dist.files\":\""+ConnectionConstants.SPARKYARNDISTFILES+"\"," +
            " \"spark.yarn.dist.jars\":\""+ConnectionConstants.SPARKYARNDISTJARS+"\"," +
            " \"livy.rsc.rpc.server.address\":\""+ConnectionConstants.LIVYRPCSERVERADDRESS+"\", \"spark.yarn.stagingDir\":\""+ConnectionConstants.SPARKYARNSTAGINGDIR+"\"," +
            " \"spark.jars\":\""+ConnectionConstants.SPARKJARS+"\"," +
            " \"spark.serializer\":\""+ConnectionConstants.SPARKSERIALIZER+"\"," +
	    " \"spark.kryoserializer.buffer.max\":\"256m\"," +
	    //" \"spark.eventLog.buffer.kb\":\"1024k\"," +
	    //" \"spark.network.io.preferDirectBufs\":\"false\"," +
	    //" \"spark.sql.execution.pandas.udf.buffer.size\":\"1024k\"," +
            " \"spark.kryo.registrator\":\""+ConnectionConstants.SPARKKRYOREGISTRATOR+"\"," +
            " \"spark.extraListeners\":\""+ConnectionConstants.SPARKEXTRALISTENERS+"\"}}";




    public static void main(String[] args) {
        try {
            System.out.println("tester");
            OkHttpClient client = new OkHttpClient();

            List<String> outputs = new ArrayList<String>(args.length-1);
            for(int i=1;i<args.length;i++){
                outputs.add(args[i]);
            }
            //System.out.println(createResponse.files.values());
            String sessionURL = LivyHelper.createSession();

            String statementsURL = sessionURL + "/statements";

            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET hive.exec.dynamic.partition = true"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET hive.exec.dynamic.partition.mode = nonstrict"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET hive.exec.max.dynamic.partitions = 4000"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET hive.exec.max.dynamic.partitions.pernode = 2000"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET spark.sql.inMemoryColumnarStorage.compressed = true"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET spark.sql.crossJoin.enabled=true"), statementsURL, client);//for self-spatial joins on geometry table
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET spark.sql.parquet.filterPushdown = true"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("geospark.join.spatitionside = none"), statementsURL, client);
            sendCommandAndPrint(getSQLQuery("use test"), statementsURL, client);
            for (String udf : udfs) {
                String lib = "{\"code\":\"spark.sessionState.functionRegistry.createOrReplaceTempFunction(\\\""+udf+"\\\",org.apache.spark.sql.geosparksql.expressions."+udf+");\"}";
                sendCommandAndPrint(lib, statementsURL, client);
            }
            for (String udf : aggregateUdfs) {
                String lib = "{\"code\":\"spark.udf.register(\\\""+udf+"\\\",new org.apache.spark.sql.geosparksql.expressions."+udf+");\"}";
                sendCommandAndPrint(lib, statementsURL, client);
            }

            //JsonParser parser = sendCommandAndGetBuffer(t, getSQLQuery("select s as s1, o as o1 from prop11"), statementsURL, client);
            JsonParser parser = sendCommandAndGetBuffer(getSQLQuery(args[0]), statementsURL, client);
            if(parser!=null) {
                Event event = parser.next();
                int noOfResults = 0;
                while (event != Event.END_ARRAY) {
                    DocumentContext tuple = JsonPath.parse(parser.getString());
                    for (String output : outputs) {
                        String col = tuple.read(output).toString();
                        if (noOfResults == 0) {
                            System.out.println(output + ":" + col);
                        }
                    }
                    //System.out.println("next: " + parser.getString() + event);
                    noOfResults++;
                    event = parser.next();
                }
                System.out.println("no of results: " + noOfResults);
                parser.close();
                //t.response2.close();
            }
            LivyHelper.closeSession(sessionURL);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void sendCommandAndPrint(String command, String statementsURL, OkHttpClient client) throws IOException {
        //String json = t.getSQLQuery(command);
        log.debug("Sending command: "+command);
        String response = post(statementsURL, command, client);
        System.out.println(response);
        int stID = JsonPath.parse(response).read("id");

        String statementURL = statementsURL + "/" + stID;
        System.out.println("statement URL:" + statementURL);
        String status = "";
        Request request2 = new Request.Builder()
                .url(statementURL)
                .build();
        String result = "";
        while (true) {
            try (Response response2 = client.newCall(request2).execute()) {
                result = response2.body().string();
                status = JsonPath.parse(result).read("state");
                System.out.println("status: " + status);
                if (status.equals("available"))
                    break;
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.println(result);
    }

    protected static JsonParser sendCommandAndGetBuffer(String command, String statementsURL, OkHttpClient client) throws IOException {
        //String json = t.getSQLQuery(command);
        log.debug("Sending command: "+command);
        String response = post(statementsURL, command, client);
        System.out.println(response);
        int stID = JsonPath.parse(response).read("id");

        String statementURL = statementsURL + "/" + stID;
        System.out.println("statement URL:" + statementURL);
        Request request2 = new Request.Builder()
                .url(statementURL)
                .build();
        //Reader result = null;
        //String firstLine = "";
        BufferedReader buf = null;
        while (true) {
            Response response2 = client.newCall(request2).execute();
            Reader result = response2.body().charStream();
            buf = new BufferedReader(result);


            JsonParser parser = Json.createParser(buf);
            String state = "";
            int id;
            while (parser.hasNext()) {
                Event event = parser.next();
                if (event == Event.KEY_NAME) {
                    String eventName = parser.getString();
                    System.out.println("event:" + eventName);
                    switch (eventName) {
                        case "state":
                            parser.next();
                            state = parser.getString();
                            System.out.println("state: " + state);
                            break;
                        case "id":
                            parser.next();
                            id = parser.getInt();
                            System.out.println("id: " + id);
                            break;
                        case "output":
                            event = parser.next();
                            if (state.equals("available")) {
                                if (event == Event.START_OBJECT) {
                                    while (parser.hasNext()) {
                                        event = parser.next();
                                        String key = parser.getString();
                                        if (key.equals("status")) {
                                            parser.next();
                                            String status = parser.getString();
                                            if (!status.equals("ok")) {
                                                System.out.println("STATUS NOT OK:" + status);
                                                while(parser.hasNext()){
                                                    event = parser.next();
                                                    key = parser.getString();
                                                    if(key.equals("evalue")){
                                                        System.out.println(parser.getString());
                                                    }
                                                }
                                                return null;

                                            }
                                        }
                                        if (key.equals("data")) {
                                            event = parser.next();
                                            if (event == Event.START_OBJECT) {
                                                event = parser.next();//application/json
                                                System.out.println("value1: " + parser.getString());
                                                event = parser.next();//start array
                                                    /*Event event1 = parser.next();
                                                    int noOfResults=0;
                                                    while(event1 != Event.END_ARRAY){
                                                        //System.out.println("next: " + parser.getString() + event);
                                                        noOfResults++;
                                                        event1 = parser.next();
                                                    }
                                                    System.out.println("no of results: "+noOfResults);*/
                                                return parser;

                                            }
                                        }

                                    }
                                }

                            }
                            break;
                        default:
                            event = parser.next();
                            if (event == Event.VALUE_STRING ||
                                    event == Event.VALUE_NUMBER) {
                                System.out.println("value: " + parser.getString());
                            }
                    }
                }
            }
            parser.close();
            response2.close();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }


}


/*  Reader r = sendCommand(t, lib, statementsURL, client);
                BufferedReader reader;
                reader = new BufferedReader(r);
                String line = reader.readLine();
                while (line != null) {
                    System.out.println(line);
                    line=reader.readLine();
                }
                reader.close(); */
