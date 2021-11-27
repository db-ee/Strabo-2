package sesameWrapper;

import it.unibz.krdb.obda.io.ModelIOManager;
import it.unibz.krdb.obda.model.OBDADataFactory;
import it.unibz.krdb.obda.model.OBDAException;
import it.unibz.krdb.obda.model.OBDAModel;
import it.unibz.krdb.obda.model.impl.OBDADataFactoryImpl;
import it.unibz.krdb.obda.owlrefplatform.core.QuestConstants;
import it.unibz.krdb.obda.owlrefplatform.core.QuestPreferences;
import it.unibz.krdb.obda.owlrefplatform.core.StrabonStatement;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWL;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLConfiguration;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLFactory;
import it.unibz.krdb.obda.strabon.LocalQueryTranslator;
import it.unibz.krdb.obda.utils.StrabonParameters;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.slf4j.spi.LocationAwareLogger;
//import java.lang.reflect.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class StrabonLivyRepo implements Repository {

    private static final Logger log = LoggerFactory.getLogger(StrabonLivyRepo.class);
    static StringBuffer obdaFile;
    static String propDictionary;
    static String queriesPath;
    static String statfile;
    static String asWKTTablesFile;
    private static Map<String, String> asWKTSubpropertiesToTables;
    private StrabonStatement st;
    private Map<String, String> namespaces;
    private boolean isInitialized;
    private String statementsURL;

    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    private String sessionURL;

    
    
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

    public StrabonLivyRepo(String propDictionary, String statFile, String asWKTTablesFile)
            throws Exception {
        super();
        this.propDictionary = propDictionary;
        this.statfile = statFile;
        this.asWKTTablesFile = asWKTTablesFile;
        asWKTSubpropertiesToTables = new HashMap<String, String>();
        namespaces = new HashMap<>();
        this.st = null;
        this.isInitialized = false;
	
	System.setProperty("org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY", "INFO");	
	/*try
    	{
        Logger l = LoggerFactory.getLogger("com.jayway.jsonpath.internal.path.CompiledPath");  
        Field f = l.getClass().getSuperclass().getDeclaredField("currentLogLevel");
        f.setAccessible(true);
        f.set(l, LocationAwareLogger.WARN_INT);
    	}
    	catch (Exception e)
    	{
        log.warn("Failed to reset the log level", e);
    	}*/

    }

    @Override
    public void setDataDir(File file) {

    }

    @Override
    public File getDataDir() {
        return null;
    }

    public void initialize() throws RepositoryException {
        try {


            //spark.sql("SET spark.sql.hive.metastore.version = 2.3.3");
            //spark.sql("SET spark.sql.warehouse.dir = hdfs://pyravlos3:9001/user/hive/warehouse");
            //spark.sql("SET spark.sql.hive.metastore.jars = /home/hadoop/SingleNodeYarnSparkHiveHDFSCluster/hive/lib/*");
            //spark.sql("SET spark.hadoop.datanucleus.fixedDatastore = true");
            //spark.sql("SET  spark.hadoop.datanucleus.autoCreateSchema =false");

            OkHttpClient client = new OkHttpClient();
            this.sessionURL = LivyHelper.createSession();
            this.statementsURL = sessionURL + "/statements";

            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET spark.serializer = org.apache.spark.serializer.KryoSerializer"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET spark.kryo.registrator = org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET hive.exec.dynamic.partition = true"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET hive.exec.dynamic.partition.mode = nonstrict"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET hive.exec.max.dynamic.partitions = 400"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET hive.exec.max.dynamic.partitions.pernode = 200"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET spark.sql.inMemoryColumnarStorage.compressed = true"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET spark.sql.crossJoin.enabled=true"), statementsURL, client);//for self-spatial joins on geometry table
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET spark.sql.parquet.filterPushdown = true"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET geospark.join.spatitionside = none"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET spark.sql.cbo.enabled = true"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET spark.sql.cbo.joinReorder.enabled = true"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET spark.sql.autoBroadcastJoinThreshold = -1"), statementsURL, client);
            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET spark.sql.inMemoryColumnarStorage.batchSize = 20000"), statementsURL, client);
	    LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SET spark.kryoserializer.buffer.max = 128m"), statementsURL, client);

            LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("use " + ConnectionConstants.DATABASENAME), statementsURL, client);
            
            //register spatial join detector 
            String reg = "{\"code\":\"spark.experimental.extraStrategies = org.apache.spark.sql.geosparksql.strategy.join.JoinQueryDetector :: Nil;\"}";
            LivyHelper.sendCommandAndPrint(reg, statementsURL, client);
            
            for (String udf : LivyHelper.udfs) {
                String lib = "{\"code\":\"spark.sessionState.functionRegistry.createOrReplaceTempFunction(\\\"" + udf + "\\\",org.apache.spark.sql.geosparksql.expressions." + udf + ");\"}";
                LivyHelper.sendCommandAndPrint(lib, statementsURL, client);
            }
            for (String udf : LivyHelper.aggregateUdfs) {
                String lib = "{\"code\":\"spark.udf.register(\\\"" + udf + "\\\",new org.apache.spark.sql.geosparksql.expressions." + udf + ");\"}";
                LivyHelper.sendCommandAndPrint(lib, statementsURL, client);
            }

            try {
                log.debug("Reading othet WKT tables from file: " + asWKTTablesFile);
                //Path asWKT = new Path(asWKTTablesFile);
                String asWKTFile = LocalQueryTranslator.readFile(asWKTTablesFile);
                for (String nextProp : asWKTFile.split("\n")) {
                    asWKTSubpropertiesToTables.put(nextProp, null);
                }
            } catch (Exception fnf) {
                log.error("Could not read other WKT properties file " + fnf.getMessage());

            }
            // TODO Auto-generated method stub
            log.debug("Reading dictionary from file " + propDictionary);
            Map<String, String> predDictionary = LocalQueryTranslator.readPredicates(propDictionary);
            log.debug("property dictionary: " + predDictionary.toString());
            boolean existDefaultGeometrytable = createObdaFile(predDictionary);

            if (existDefaultGeometrytable) {
                // preload geometeries
                log.debug("preloading geometries");
                String createGeometriesTable = "Create temporary view " + StrabonParameters.GEOMETRIES_TABLE + " AS (Select " + StrabonParameters.GEOMETRIES_FIRST_COLUMN + ", "
                        + StrabonParameters.GEOMETRIES_SECOND_COLUMN + ", ST_GeomFromWKT("
                        + StrabonParameters.GEOMETRIES_THIRD_COLUMN + ") as "
                        + StrabonParameters.GEOMETRIES_THIRD_COLUMN + " FROM geometries where " +
                        StrabonParameters.GEOMETRIES_THIRD_COLUMN + " IS NOT NULL)";
                LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery(createGeometriesTable), statementsURL, client);
                LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("CACHE TABLE " + StrabonParameters.GEOMETRIES_TABLE), statementsURL, client);
                LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SELECT COUNT(*) FROM " + StrabonParameters.GEOMETRIES_TABLE), statementsURL, client);

            }
            log.debug("preloading asWKT subproperty tables: " + asWKTSubpropertiesToTables.toString());
            for (String asWKTsubprop : asWKTSubpropertiesToTables.keySet()) {
                String tblName = asWKTSubpropertiesToTables.get(asWKTsubprop);

                LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("Create temporary view " + tblName + " AS (Select s, ST_GeomFromWKT(o) as o FROM " + predDictionary.get(asWKTsubprop) + ") "), statementsURL, client);
                LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("CACHE TABLE " + tblName), statementsURL, client);
                LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SELECT COUNT(*) FROM " + tblName), statementsURL, client);

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
            p.setCurrentValueOf(QuestPreferences.USE_TEMPORARY_SCHEMA_NAME, QuestConstants.FALSE);
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
                nse = new NodeSelectivityEstimator(statfile);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            st = reasoner.createStrabonStatement(nse);
            st.useThematicCache(ConnectionConstants.USECACHE);
            st.setWKTTables(asWKTSubpropertiesToTables.keySet());
			/*List<String> sparqlQueries = new ArrayList<String>();

			Path path = new Path(queriesPath);
			log.debug("reading queries from " + queriesPath);
			if (fs.isDirectory(path)) {
				FileStatus[] fileStatuses = fs.listStatus(path);

				for (FileStatus fileStatus : fileStatuses) {
					if (fileStatus.isFile() && fileStatus.getPath().toString().endsWith(".q")) {
						sparqlQueries.add(QueryExecutor.readHadoopFile(fileStatus.getPath(), fs));
					}

				}
			}*/

            // String[] query_files =
            // readFilesFromDir("/home/dimitris/spatialdbs/queries/");


            // TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, preds);
            // TupleQueryResultHandler handler = new SPARQLResultsTSVWriter(System.out);

            // tupleQuery.evaluate(handler);

            //System.out.println("Closing...");
            isInitialized = true;


        } catch (Exception e1) {
            log.debug("Error: " + e1.getMessage());
            e1.printStackTrace();
            shutDown();
            throw new RepositoryException(e1.getMessage());
        }
    }

    @Override
    public boolean isInitialized() {
        return isInitialized;
    }


    public StrabonLivyConnection getConnection() throws RepositoryException {
        StrabonLivyConnection con = null;
        try {
            con = new StrabonLivyConnection(this, statementsURL, st);
        } catch (OBDAException e) {
            e.printStackTrace();
            throw new RepositoryException(e.getMessage());
        }
        return con;
    }


    public boolean isWritable() throws RepositoryException {
        // Checks whether this repository is writable, i.e.
        // if the data contained in this repository can be changed.
        // The writability of the repository is determined by the writability
        // of the Sail that this repository operates on.
        return false;
    }


    public void shutDown() throws RepositoryException {
        try {
            LivyHelper.closeSession(this.sessionURL);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getType() {
        return QuestConstants.VIRTUAL;
    }

    public ValueFactory getValueFactory() {
        // Gets a ValueFactory for this Repository.
        return ValueFactoryImpl.getInstance();
    }

    public void setNamespace(String key, String value) {
        namespaces.put(key, value);
    }

    public String getNamespace(String key) {
        return namespaces.get(key);
    }

    public Map<String, String> getNamespaces() {
        return namespaces;
    }

    public void setNamespaces(Map<String, String> nsp) {
        this.namespaces = nsp;
    }

    public void removeNamespace(String key) {
        namespaces.remove(key);
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
                obdaFile.append(StrabonParameters.GEOMETRIES_TABLE);
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
                obdaFile.append(StrabonParameters.GEOMETRIES_TABLE);
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
                obdaFile.append(tablename);
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


}
