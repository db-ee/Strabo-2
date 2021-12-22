package it.unibz.krdb.obda.strabon;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import it.unibz.krdb.obda.io.ModelIOManager;
import it.unibz.krdb.obda.model.OBDADataFactory;
import it.unibz.krdb.obda.model.OBDAModel;
import it.unibz.krdb.obda.model.impl.OBDADataFactoryImpl;
import it.unibz.krdb.obda.owlrefplatform.core.QuestConstants;
import it.unibz.krdb.obda.owlrefplatform.core.QuestPreferences;
import it.unibz.krdb.obda.owlrefplatform.core.SQLResult;
import it.unibz.krdb.obda.owlrefplatform.core.StrabonStatement;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWL;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLConfiguration;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLFactory;
import it.unibz.krdb.obda.utils.StrabonParameters;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geosparksql.utils.Adapter;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

//import org.openrdf.query.resultio.sparqlkml.stSPARQLResultsKMLWriter;

public class QueryExecutor {

    private static final Logger log = LoggerFactory.getLogger(QueryExecutor.class);
    private static StringBuffer obdaFile;
    private static String propDictionary;
    private static String queriesPath;
    private static String database;
    private static String statfile;
    private static String asWKTTablesFile;
    private static boolean splitSpatialJoin;
    private static Map<String, String> asWKTSubpropertiesToTables;
    private static boolean analyze;
    private static boolean cacheThematicTables;
    private static int shufflePartitions;

    //the following contain properties that have literls as object for each kind of literal
    //TODO read these from a file
    public static final Set<String> STRINGPROPERTIES = new HashSet<>(Arrays.asList("http://ai.di.uoa.gr/polar/ontology/hasCT",
            "http://ai.di.uoa.gr/polar/ontology/hasURL",
            "http://ai.di.uoa.gr/polar/ontology/hasThumbnail",
            "http://ai.di.uoa.gr/polar/ontology/hasCT",
            "http://ai.di.uoa.gr/polar/ontology/hasCTClassName",
            "http://ai.di.uoa.gr/fs/ontology/hasClassName",
            "http://ai.di.uoa.gr/polar/ontology/hasTitle",
            "http://geographica.di.uoa.gr/generator/landOwnership/hasKey",
            "http://geographica.di.uoa.gr/generator/pointOfInterest/hasKey",
            "http://geographica.di.uoa.gr/generator/state/hasKey",
            "http://geographica.di.uoa.gr/generator/road/hasKey",
            "http://ai.di.uoa.gr/fs/ontology/hasCropTypeName",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasLAN",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasZHYD",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasName",
	    "http://ai.di.uoa.gr/gadm/ontology/hasName",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasCATCH_AREA",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasHYDRONODCT",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasREF_TOPO",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasREX",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasRN_I_ID",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasDFDD",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasSYSTEM_CD",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasEUWFDCODE",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasLKE_TYPE",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasNODE_ID",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasLAKINOUT",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasNAM",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasLAKID",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasBEGLIFEVER",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasDRAIN_ID",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasWBodyID",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasEU_DAM_ID",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasENDLIFEVER",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasTR",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasNEXTUPID",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasNODETYPE",
            "http://ai.di.uoa.gr/eu-hydro/ontology/hasNEXTDOWNID",
            "http://ai.di.uoa.gr/fs/ontology/hasNUTS_ID",
            "http://ai.di.uoa.gr/fs/ontology/hasCropTypeName",
            "http://ai.di.uoa.gr/fs/ontology/hasDescription",
            "http://ai.di.uoa.gr/fs/ontology/hasCNTR_CODE",
            "http://ai.di.uoa.gr/fs/ontology/hasLC1",
            "http://ai.di.uoa.gr/invekos/ontology/hasCropTypeName"));

    public static final Set<String> INTEGERPROPERTIES = new HashSet<>(Arrays.asList("http://data.linkedeodata.eu/ontology#has_code",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasMAINDR_ID",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasSEA_CD",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasNVS",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasCOMM_CD",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasCCM_ID",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasCGNELIN",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasHYP",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasFNODE",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasFUN",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasSTRAHLER",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasMC",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasPERIMETER",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasWMT",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasNUM_SEG",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasTNODE",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasWSO_ID",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasWCOURSE_ID",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasLOC",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasWINDOW",
    		"http://ai.di.uoa.gr/fs/ontology/hasLC1_SPEC",
    		"http://ai.di.uoa.gr/fs/ontology/hasCropTypeNumber",
    		"http://ai.di.uoa.gr/fs/ontology/hasID",
    		"http://ai.di.uoa.gr/fs/ontology/hasLC1_PERC",
    		"http://ai.di.uoa.gr/fs/ontology/hasOBS_DIRECT",
	    "http://ai.di.uoa.gr/fs/ontology/hasRelativeAmount"));

    public static final Set<String> DATETIMEPROPERTIES = new HashSet<>(Arrays.asList("http://ai.di.uoa.gr/polar/ontology/hasRECDAT",
            "http://ai.di.uoa.gr/polar/ontology/hasRECDATE", 
            "http://ai.di.uoa.gr/fs/ontology/hasSURVEYDATE",
            "http://ai.di.uoa.gr/fs/ontology/hasEndDate",
	    "http://ai.di.uoa.gr/fs/ontology/hasStartDate"));
    
    public static final Set<String> DOUBLEPROPERTIES = new HashSet<>(Arrays.asList("http://ai.di.uoa.gr/eu-hydro/ontology/hasCUM_LEN",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasELEV",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasLONGPATH",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasAREA",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasMAINDR_CLS",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasALTITUDE",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasSHAPE_Area",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasLEN_TOM",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasDAMX",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasDAMY",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasPENTE",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasSHAPE_Length",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasLENGTH",
    		"http://ai.di.uoa.gr/fs/ontology/hasArea",
    		"http://ai.di.uoa.gr/fs/ontology/hasRelativeAmount",
    		"http://ai.di.uoa.gr/fs/ontology/hasCapabilityValue",
    		"http://ai.di.uoa.gr/eu-hydro/ontology/hasPFAFSTETER"));
    
    private static boolean cacheSpatialIndex;
    //cacheSpatialIndex will create an in-memory spatial index during initialization
    //and it will use it for spatial selections
    private static  boolean useQualitiveSpatialCache;
    //useQualitiveSpatialCache that the qualitive spatial relations have been
    //computed by JedAI-spatial and stored in table tblde9im in Hive

    public static void main(String[] args) throws Exception {
        {
            propDictionary = args[0];
            queriesPath = args[1];
            database = args[2];
            statfile = args[3];
            asWKTTablesFile = args[4];
            splitSpatialJoin = false;
            if (args.length > 5) {
                splitSpatialJoin = args[5].equals("true");
            }
            cacheThematicTables = false;
            if (args.length > 6) {
                cacheThematicTables = args[6].equals("true");
            }
            asWKTSubpropertiesToTables = new HashMap<String, String>();
            analyze = false;

            shufflePartitions = 200;
            if (args.length > 7) {
                shufflePartitions = Integer.parseInt(args[7]);
            }
            cacheSpatialIndex=false;
            if (args.length > 8) {
                cacheSpatialIndex = args[8].equals("true");
            }
            useQualitiveSpatialCache = false;
            if(args.length > 9 ) {
                useQualitiveSpatialCache = args[9].equals("true");
            }
            //cache = true;


            List<String> tempTables = new ArrayList<String>();
            try {

                final SparkSession spark = SparkSession.builder()
                        // .master("local[*]") // Delete this if run in cluster mode
                        .appName("strabonQuery")
                        // Enable GeoSpark custom Kryo serializer
                        .config("spark.serializer", KryoSerializer.class.getName())
                        .config("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName())
                        //.config("geospark.join.numpartition",2000)
                        //.config("spark.default.parallelism", "800")
                        //.config("spark.sql.shuffle.partitions", "800")
                        .config("geospark.join.spatitionside", "none")
                        .config("spark.sql.cbo.enabled", true)
                        .config("spark.sql.cbo.joinReorder.enabled", true)
                        .config("spark.sql.inMemoryColumnarStorage.compressed", true)
                        .config("hive.exec.dynamic.partition", true).config("spark.sql.parquet.filterPushdown", true)
                        .config("spark.sql.inMemoryColumnarStorage.batchSize", 20000).enableHiveSupport().getOrCreate();

                spark.sql("SET spark.sql.cbo.enabled = true");
                spark.sql("SET spark.sql.cbo.joinReorder.enabled = true");
                spark.sql("SET hive.exec.dynamic.partition = true");
                spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict");
                spark.sql("SET hive.exec.max.dynamic.partitions = 400");
                spark.sql("SET hive.exec.max.dynamic.partitions.pernode = 200");
                spark.sql("SET spark.sql.inMemoryColumnarStorage.compressed = true");
                spark.sql("SET spark.sql.crossJoin.enabled=true");//for self-spatial joins on geometry table
                spark.sql("SET spark.sql.parquet.filterPushdown = true");
                spark.sql("SET spark.sql.shuffle.partitions = " + shufflePartitions);
                spark.sql("USE " + database);
                GeoSparkSQLRegistrator.registerAll(spark);

                FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());

                try {
                    log.debug("Reading file: " + asWKTTablesFile);
                    Path asWKT = new Path(asWKTTablesFile);
                    String asWKTFile = readHadoopFile(asWKT, fs);
                    for (String nextProp : asWKTFile.split("\n")) {
                        asWKTSubpropertiesToTables.put(nextProp, null);
                    }
                } catch (Exception fnf) {
                    log.error("Could not read other WKT properties file " + fnf.getMessage());

                }

                Map<String, String> predDictionary = readPredicatesFromHadoop(propDictionary, fs);
                log.debug("property dictionary: " + predDictionary.toString());
		
		Map<String, String> prefixes = new HashMap<>();
                //key is the prefix
                if (spark.catalog().tableExists(database,"nsprefixes")) {
                    //read prefixes from table
                    List<Row> dataRows = spark.sql("Select * from nsprefixes").collectAsList();
                    for (Row row : dataRows) {
                        prefixes.put(row.getString(0), row.getString(1));
                    }
                    log.debug("Prefixes Read: "+prefixes);
                    //transform properties back to full IRI
                    log.debug("Transform properties back to full IRI");
                    Map<String, String> modifiedPredDictionary = new HashMap<>(predDictionary.size());
                    for(String iri:predDictionary.keySet()) {
                        for(String prefix:prefixes.keySet()) {
                            if (iri.contains(prefix+ ":" )){
                                String replaced = iri.replace(prefix + ":", prefixes.get(prefix));
                                modifiedPredDictionary.put(replaced, predDictionary.get(iri));
                                log.debug("Replacing " + iri + " with " + replaced );
                                break;
                            }
                        }
                        modifiedPredDictionary.put(iri, predDictionary.get(iri));
                    }
                    predDictionary = modifiedPredDictionary;
                    log.debug("Predicate Dictionary changed: \n" + predDictionary);
                }

                boolean existDefaultGeometrytable = createObdaFile(predDictionary);
                
                Map<String, SpatialRDD<Geometry>> cachedIndexes = null;
                if(cacheSpatialIndex) {
                    cachedIndexes = new HashMap<>();
                }

                if (existDefaultGeometrytable) {
                    // preload geometeries
                    log.debug("preloading geometries");
                    Dataset<Row> geoms = spark.sql("Select " + StrabonParameters.GEOMETRIES_FIRST_COLUMN + ", "
                            + StrabonParameters.GEOMETRIES_SECOND_COLUMN + ", ST_GeomFromWKT("
                            + StrabonParameters.GEOMETRIES_THIRD_COLUMN + ") as "
                            + StrabonParameters.GEOMETRIES_THIRD_COLUMN + " FROM geometries where " +
                            StrabonParameters.GEOMETRIES_THIRD_COLUMN + " IS NOT NULL");
                    geoms.createOrReplaceGlobalTempView(StrabonParameters.GEOMETRIES_TABLE);
                    geoms.cache();
                    long count = geoms.count();
                    log.debug("Geometry table " + StrabonParameters.GEOMETRIES_TABLE + " created with " + count + " rows");

                }
		for (String asWKTsubprop : asWKTSubpropertiesToTables.keySet()) {
                    String tblName = asWKTSubpropertiesToTables.get(asWKTsubprop);
                    log.debug("preloading asWKT subproperty tables");
                    Dataset<Row> geoms = spark
                            .sql("Select s, ST_GeomFromWKT(o) as o FROM " + predDictionary.get(asWKTsubprop) + " ");
                    if(cacheSpatialIndex) {
                        long start = System.currentTimeMillis();
                        SpatialRDD<Geometry> spatialRDD = Adapter.toSpatialRdd(geoms, "o");
                        spatialRDD.buildIndex(IndexType.QUADTREE, false);
                        spatialRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY());
                        spatialRDD.indexedRawRDD.cache();
			spatialRDD.indexedRawRDD.count();//force caching
                        //spatialRDD.analyze();
                        cachedIndexes.put(tblName, spatialRDD);
                        log.debug("Caching index for geometry table " + tblName + "(" + asWKTsubprop +
                                ") finished in "+ (System.currentTimeMillis()-start) + " milliseconds");
                    }
                    geoms.createOrReplaceGlobalTempView(tblName);
                    geoms.cache();
                    long count = geoms.count();
                    //analyze is not suported in views...
                    //spark.sql(" ANALYZE TABLE " + tblName + " COMPUTE STATISTICS NOSCAN");
                    log.debug("Geometry table " + tblName + " created with " + count + " rows");

                }



                if (analyze) {
                    log.debug("Analyzing tables...");
                    for (String table : predDictionary.values()) {
                        spark.sql(" ANALYZE TABLE " + table + " COMPUTE STATISTICS");
                    }
                }

                OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
                OWLOntology ontology = manager.createOntology(); // empty ontology

                OBDAModel obdaModel;
                OBDADataFactory fac = OBDADataFactoryImpl.getInstance();
                obdaModel = fac.getOBDAModel();
                ModelIOManager ioManager = new ModelIOManager(obdaModel);
                ioManager.load(new ByteArrayInputStream(obdaFile.toString().getBytes()));

                QuestPreferences p = new QuestPreferences();
                p.setCurrentValueOf(QuestPreferences.ABOX_MODE, QuestConstants.VIRTUAL);
                p.setCurrentValueOf(QuestPreferences.OBTAIN_FULL_METADATA, QuestConstants.FALSE);
                p.setCurrentValueOf(QuestPreferences.SQL_GENERATE_REPLACE, QuestConstants.FALSE);
                p.setCurrentValueOf(QuestPreferences.REWRITE, QuestConstants.FALSE);
                p.setCurrentValueOf(QuestPreferences.USE_TEMPORARY_SCHEMA_NAME, QuestConstants.TRUE);
                // p.setCurrentValueOf(QuestPreferences.DBTYPE, QuestConstants.PANTELIS);
                // p.setCurrentValueOf(QuestPreferences.DISTINCT_RESULTSET,
                // QuestConstants.TRUE);
                // p.setCurrentValueOf(QuestPreferences.REFORMULATION_TECHNIQUE,
                // QuestConstants.TW);

                // Creating the instance of the reasoner using the factory. Remember
                // that the RDBMS that contains the data must be already running and
                // accepting connections.
                QuestOWLConfiguration.Builder configBuilder = QuestOWLConfiguration.builder();
                configBuilder.obdaModel(obdaModel);
                configBuilder.preferences(p);
                QuestOWLConfiguration config = configBuilder.build();
                QuestOWLFactory factory = new QuestOWLFactory();
                factory.setPreferenceHolder(p);
                factory.setOBDAController(obdaModel);
                // QuestOWLConfiguration config =
                // QuestOWLConfiguration.builder().obdaModel(obdaModel).preferences(p).build();
                QuestOWL reasoner = factory.createReasoner(ontology, config);

                /// query repo
                NodeSelectivityEstimator nse = null;
                try {
                    Path path = new Path(statfile);
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                    nse = new NodeSelectivityEstimator(br);
                } catch (Exception e) {
                    log.debug("Could not read statistics file");
                    e.printStackTrace();
                }
                StrabonStatement st = reasoner.createStrabonStatement(nse);
                Map<String, String> predDictionaryStat = null;
                try {
                    predDictionaryStat = readPredicatesFromHadoop(propDictionary+".stat", fs);
                } catch (Exception e) {
                    log.debug("Could not read propDictionary statistics file");
                    e.printStackTrace();
                }
                st.setPredicateDictionaryForStatistics(predDictionaryStat);
                st.setCacheSpatialIndex(cacheSpatialIndex);
                log.debug("Stat pred dictionary: \n"+predDictionaryStat);
                st.setWKTTables(asWKTSubpropertiesToTables.keySet());
                st.useThematicCache(cacheThematicTables);
                st.setUseQualititveSpatialCache(useQualitiveSpatialCache);
                List<String> sparqlQueries = new ArrayList<String>();
                List<String> sqlQueries = new ArrayList<String>();

                Path path = new Path(queriesPath);
                log.debug("reading queries from " + queriesPath);
                if (fs.isDirectory(path)) {
                    FileStatus[] fileStatuses = fs.listStatus(path);

                    for (FileStatus fileStatus : fileStatuses) {
                        if (fileStatus.isFile() && ( fileStatus.getPath().toString().endsWith(".q") 
							|| fileStatus.getPath().toString().endsWith(".qry"))) {
                            sparqlQueries.add(readHadoopFile(fileStatus.getPath(), fs));
                        }
                        if (fileStatus.isFile() && fileStatus.getPath().toString().endsWith(".sql")) {
                            sqlQueries.add(readHadoopFile(fileStatus.getPath(), fs));
                        }

                    }
                }
		long totalTime = 0l;
		int noOfQueries = 0;
                String exec = "";
                // String[] query_files =
                // readFilesFromDir("/home/dimitris/spatialdbs/queries/");
                for (String sparql : sparqlQueries) {
                    try {
                        //delete old temp tables
                        for (int k = 0; k < tempTables.size(); k++) {
                            try {
                                spark.sql("DROP VIEW " + StrabonParameters.TEMPORARY_SCHEMA_NAME + "." + tempTables.get(k));
                            } catch (Exception ex) {
                                log.error("Could not delete table " + tempTables.get(k) + ". ");
                                ex.printStackTrace();
                            }
                        }
                        tempTables.clear();
                        // String sparql = readFile(queryfile);
                        log.debug("Start Executing SPARQL query: " + sparql);
                        exec += "Executing SPARQL query: " + sparql + "\n";
                        SQLResult sql = st.getUnfolding(sparql, splitSpatialJoin);
                        if (cacheThematicTables) {
                            for (String tableToCache : st.getTablesToCache()) {
                                spark.sql(tableToCache);
                            }
                        }
                        log.debug("Query unfolded:" + sql.getTextResult() + "\n");
                        log.debug("Starting execution");
                        long start = System.currentTimeMillis();
                        // List<String> tempnames=new ArrayList<String>();
                        boolean emptyResult = false;
                        for (int k = 0; k < sql.getTempQueries().size(); k++) {
                            String temp = sql.getTempQueries().get(k).replaceAll("\"", "");
                            log.debug("creating temp table " + sql.getTempName(k) + " with query: " + temp);
                            Dataset<Row> tempDataset = spark.sql(temp);
                            tempDataset.createOrReplaceGlobalTempView(sql.getTempName(k));
                            tempDataset.cache();
                            if (tempDataset.isEmpty()) {
                                log.debug("empty temp query: " + sql.getTempName(k));
                                //return empty result
                                log.debug("Execution finished in " + (System.currentTimeMillis() - start) + " with 0 results.");
                                exec += "Execution finished in " + (System.currentTimeMillis() - start) + " with 0 results.\n";
                                emptyResult = true;
                                break;
                            }
                            //long c = tempDataset.count();
                            //log.debug("temp table " + sql.getTempName(k) + "had " + c + " results");
                            tempTables.add(sql.getTempName(k));
                        }
                        if (emptyResult) {
                            continue;
                        }

                        if(sql.getSpatialTable() != null ) {
                            //GeometryFactory gf = new GeometryFactory();
                            WKTReader r = new WKTReader();
                            log.debug("Spatial Filter for: "+sql.getSpatialFilterConstant().getValue());

                            Geometry g = r.read(sql.getSpatialFilterConstant().getValue());
                            log.debug("Cached Indexes: "+cachedIndexes);
                            log.debug("spatial table to remove: "+sql.getSpatialTableToRemove().getFunctionSymbol().getName());
                            JavaRDD rdd = RangeQuery.SpatialRangeQuery(cachedIndexes.get(sql.getSpatialTableToRemove().getFunctionSymbol().getName().replace(StrabonParameters.TEMPORARY_SCHEMA_NAME+".","")), g, sql.isCondsiderSpatialBoundary(), true);
			    //JavaRDD rdd = RangeQuery.SpatialRangeQuery(cachedIndexes.get(sql.getSpatialTableToRemove().getFunctionSymbol().getName().replace(StrabonParameters.TEMPORARY_SCHEMA_NAME+".","")), g, true, true);
                            //gf.createGeometry(g);
                            SpatialRDD mySpatialRDD = new SpatialRDD();
                            List<String> fieldNames = new ArrayList<String>(1);
                            //fieldNames.add("o");
                            fieldNames.add("s");
                            mySpatialRDD.rawSpatialRDD = rdd;
                            log.debug("previous field names: "+mySpatialRDD.fieldNames);
                            mySpatialRDD.fieldNames = fieldNames;
                            log.debug("new spatial table:"+sql.getSpatialTable());
                            Adapter.toDf(mySpatialRDD, spark).createGlobalTempView(sql.getSpatialTable());
                            //String temp = "table"+Util.createUniqueId();
                            //Adapter.toDf(mySpatialRDD, spark).createGlobalTempView(temp);
                            //Dataset<Row> filter = spark.sql("Select geometry as o, _c1 as s from "+StrabonParameters.TEMPORARY_SCHEMA_NAME + "." +temp);
                            //filter.createGlobalTempView(sql.getSpatialTable());
                            log.debug("done executing spatial filter");

                        }
                        String sqlString = sql.getMainQuery().replaceAll("\"", "");
                        if(sql.getSpatialTable() != null ) {
                            sqlString = sqlString.replace(sql.getSpatialTableToRemove().getFunctionSymbol().getName(), StrabonParameters.TEMPORARY_SCHEMA_NAME +"."+sql.getSpatialTable());


                        }
                        log.debug("Executing main SQL: "+sqlString);
                        Dataset<Row> result = spark.sql(sqlString);
                        //result.cache();
                        long resultSize = result.count();
			totalTime += (System.currentTimeMillis() - start);
			noOfQueries++;
                        log.debug("Execution finished in " + (System.currentTimeMillis() - start) + " with "
                                + resultSize + " results.");
                        exec += "Execution finished in " + (System.currentTimeMillis() - start) + " with "
                                + resultSize + " results. \n";
                        //result.show(false);
                        for (int k = 0; k < sql.getTempQueries().size(); k++) {
                            //spark.sql("DROP VIEW globaltemp."+sql.getTempName(k));
                        }
                    } catch (Exception ex) {
                        log.error("Could not execute query " + sparql + "\nException: " + ex.getMessage());
                        exec += "Could not execute query " + sparql + "\nException: " + ex.getMessage() + "\n";
                        ex.printStackTrace();
                    }

                }


                for (String sql : sqlQueries) {
                    try {

                        log.debug("Start Executing SQL query: " + sql);
                        exec += "Executing SPARQL query: " + sql + "\n";
                        log.debug("Strating execution");
                        long start = System.currentTimeMillis();

                        Dataset<Row> result = spark.sql(sql.replaceAll("\"", ""));
                        //result.cache();
                        long resultSize = result.count();
			totalTime += (System.currentTimeMillis() - start);
                        noOfQueries++;
                        log.debug("Execution finished in " + (System.currentTimeMillis() - start) + " with "
                                + resultSize + " results.");
                        exec += "Execution finished in " + (System.currentTimeMillis() - start) + " with "
                                + resultSize + " results. \n";

                    } catch (Exception ex) {
                        log.error("Could not execute query " + sql + "\nException: " + ex.getMessage());
                        exec += "Could not execute query " + sql + "\nException: " + ex.getMessage() + "\n";
                        ex.printStackTrace();
                    }

                }

		log.debug("Executed " + noOfQueries + " queries in " + totalTime + " ms");

                // TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, preds);
                // TupleQueryResultHandler handler = new SPARQLResultsTSVWriter(System.out);

                // tupleQuery.evaluate(handler);

                System.out.println("Closing...");
                save_exec(exec, fs);
            } catch (Exception e1) {
                log.debug("Error: " + e1.getMessage());
                throw (e1);
            }

            System.out.println("Done.");
        }

    }

    public static boolean createObdaFile(Map<String, String> predDictionary) throws SQLException, IOException {
        boolean existsGeometryTable = false;
        obdaFile = new StringBuffer();
        obdaFile.append("[PrefixDeclaration]");
        obdaFile.append("\n");
        obdaFile.append("geo:\thttp://www.opengis.net/ont/geosparql#");
        obdaFile.append("\n");

        obdaFile.append("\n");
        obdaFile.append("[SourceDeclaration]");
        obdaFile.append("\n");
        obdaFile.append("sourceUri\tsparql");
        obdaFile.append("\n");
        obdaFile.append("connectionUrl\tjdbc:fedadp:" + "tmp");
        obdaFile.append("\n");
        obdaFile.append("username\ttest");
        obdaFile.append("\n");
        obdaFile.append("password\ttest");
        obdaFile.append("\n");
        obdaFile.append("driverClass\tmadgik.exareme.jdbc.Spark");
        obdaFile.append("\n");

        obdaFile.append("\n");
        obdaFile.append("[MappingDeclaration] @collection [[");
        obdaFile.append("\n");

        int asWKTsubproperty = 2;
        int mappingId = 0;

        for (String property : predDictionary.keySet()) {
            if (property.equals("http://www.opengis.net/ont/geosparql#asWKT")) {
                existsGeometryTable = true;
                obdaFile.append("mappingId\tmapp");
                obdaFile.append(mappingId);
                mappingId++;
                obdaFile.append("\n");
                obdaFile.append("target\t");
                obdaFile.append("<{" + StrabonParameters.GEOMETRIES_SECOND_COLUMN + "}> ");
                obdaFile.append("<" + property + ">");
                obdaFile.append(" {" + StrabonParameters.GEOMETRIES_THIRD_COLUMN + "}^^geo:wktLiteral .\n");
                obdaFile.append("source\t");
                obdaFile.append("select " + StrabonParameters.GEOMETRIES_SECOND_COLUMN + ", "
                        + StrabonParameters.GEOMETRIES_THIRD_COLUMN + " from ");
                obdaFile.append(StrabonParameters.TEMPORARY_SCHEMA_NAME + "." + StrabonParameters.GEOMETRIES_TABLE);
                obdaFile.append("\n");
                obdaFile.append("\n");
            } else if (property.equals("http://www.opengis.net/ont/geosparql#hasGeometry")) {
                obdaFile.append("mappingId\tmapp");
                obdaFile.append(mappingId);
                mappingId++;
                obdaFile.append("\n");
                obdaFile.append("target\t");
                obdaFile.append("<{" + StrabonParameters.GEOMETRIES_FIRST_COLUMN + "}> ");
                obdaFile.append("<" + property + ">");
                obdaFile.append(" <{" + StrabonParameters.GEOMETRIES_SECOND_COLUMN + "}> .\n");
                obdaFile.append("source\t");
                obdaFile.append("select " + StrabonParameters.GEOMETRIES_FIRST_COLUMN + ", "
                        + StrabonParameters.GEOMETRIES_SECOND_COLUMN + " from ");
                obdaFile.append(StrabonParameters.TEMPORARY_SCHEMA_NAME + "." + StrabonParameters.GEOMETRIES_TABLE);
                obdaFile.append("\n");
                obdaFile.append("\n");
            } else if (INTEGERPROPERTIES.contains(property)) {
                obdaFile.append("mappingId\tmapp");
                obdaFile.append(mappingId);
                mappingId++;
                obdaFile.append("\n");
                obdaFile.append("target\t");
                obdaFile.append("<{s}> ");
                obdaFile.append("<" + property + ">");
                obdaFile.append(" {o}^^xsd:integer .\n");
                obdaFile.append("source\t");
                obdaFile.append("select s, o from ");
                obdaFile.append(predDictionary.get(property));
                obdaFile.append("\n");
                obdaFile.append("\n");

            } else if (asWKTSubpropertiesToTables.keySet().contains(property)) {
                String tablename = "tablewkt" + asWKTsubproperty;
                asWKTsubproperty++;
                asWKTSubpropertiesToTables.put(property, tablename);
                obdaFile.append("mappingId\tmapp");
                obdaFile.append(mappingId);
                mappingId++;
                obdaFile.append("\n");
                obdaFile.append("target\t");
                obdaFile.append("<{s}> ");
                obdaFile.append("<" + property + ">");
                obdaFile.append(" {o}^^geo:wktLiteral .\n");
                obdaFile.append("source\t");
                obdaFile.append("select s, o from ");
                obdaFile.append(StrabonParameters.TEMPORARY_SCHEMA_NAME + "." + tablename);
                obdaFile.append("\n");
                obdaFile.append("\n");
            } else if (STRINGPROPERTIES.contains(property)) {
                obdaFile.append("mappingId\tmapp");
                obdaFile.append(mappingId);
                mappingId++;
                obdaFile.append("\n");
                obdaFile.append("target\t");
                obdaFile.append("<{s}> ");
                obdaFile.append("<" + property + ">");
                obdaFile.append(" {o}^^xsd:string .\n");
                obdaFile.append("source\t");
                obdaFile.append("select s, o from ");
                obdaFile.append(predDictionary.get(property));
                obdaFile.append("\n");
                obdaFile.append("\n");
            } else if (DATETIMEPROPERTIES.contains(property)) {
                obdaFile.append("mappingId\tmapp");
                obdaFile.append(mappingId);
                mappingId++;
                obdaFile.append("\n");
                obdaFile.append("target\t");
                obdaFile.append("<{s}> ");
                obdaFile.append("<" + property + ">");
                obdaFile.append(" {o}^^xsd:dateTime .\n");
                obdaFile.append("source\t");
                obdaFile.append("select s, o from ");
                obdaFile.append(predDictionary.get(property));
                obdaFile.append("\n");
                obdaFile.append("\n");
            } else {
                obdaFile.append("mappingId\tmapp");
                obdaFile.append(mappingId);
                mappingId++;
                obdaFile.append("\n");
                obdaFile.append("target\t");
                obdaFile.append("<{s}> ");
                obdaFile.append("<" + property + ">");
                obdaFile.append(" <{o}> .\n");
                obdaFile.append("source\t");
                obdaFile.append("select s, o from ");
                obdaFile.append(predDictionary.get(property));
                obdaFile.append("\n");
                obdaFile.append("\n");

            }

        }
        obdaFile.append("]]");
        return existsGeometryTable;
    }

    public static Map<String, String> readPredicates(String filename) {
        Map<String, String> result = new HashMap<String, String>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filename));
            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] entry = line.split(",");
                result.put(entry[0], entry[1]);
            }
            reader.close();
        } catch (Exception e) {
            System.err.println("Error reading file: " + filename);
            e.printStackTrace();
        }
        return result;
    }

    public static Map<String, String> readPredicatesFromHadoop(String filename, FileSystem fs) throws IOException {
        Path path = new Path(filename);
        String content = readHadoopFile(path, fs);
        String[] lines = content.split("\n");
        Map<String, String> result = new HashMap<String, String>();
        for (String line : lines) {
            String[] entry = line.split(",");
            result.put(entry[0], entry[1]);
        }
        return result;
    }

    public static String readHadoopFile(Path hadoopPath, FileSystem fs) throws IOException {
        String file = "";
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hadoopPath)));
        try {
            String line;
            line = br.readLine();
            while (line != null) {
                file += line + "\n";

                line = br.readLine();
            }
        } catch (Exception ex) {
            System.out.println("Exception while reading file:" + ex.getMessage());
        } finally {
            br.close();
        }
        return file;
    }

    private static void save_exec(String executionFinished, FileSystem fs) throws IllegalArgumentException, IOException {
        //logger.info("Saving Times");
        //Configuration conf = new Configuration();
        //FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.create(new Path(statfile + ".exec"));
        byte[] bytes = executionFinished.getBytes();
        out.write(bytes, 0, bytes.length);

        out.close();

    }

}
