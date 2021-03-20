package sesameWrapper;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import it.unibz.krdb.obda.owlrefplatform.core.StrabonStatement;
import it.unibz.krdb.obda.strabon.LocalQueryTranslator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unibz.krdb.obda.io.ModelIOManager;
import it.unibz.krdb.obda.model.OBDADataFactory;
import it.unibz.krdb.obda.model.OBDAException;
import it.unibz.krdb.obda.model.OBDAModel;
import it.unibz.krdb.obda.model.impl.OBDADataFactoryImpl;
import it.unibz.krdb.obda.owlrefplatform.core.QuestConstants;
import it.unibz.krdb.obda.owlrefplatform.core.QuestPreferences;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWL;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLConfiguration;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLFactory;
import it.unibz.krdb.obda.utils.StrabonParameters;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;

public class SesameStrabonRepo implements Repository {
	
	private static final Logger log = LoggerFactory.getLogger(SesameStrabonRepo.class);
	static StringBuffer obdaFile;
	static String propDictionary;
	static String queriesPath;
	static String database;
	static String statfile;
	static String asWKTTablesFile;
	private String sparkAddress;
	private String geoSparkJars;
	private String hadoopHomeDir;
	private SparkSession spark;
	private static Map<String, String> asWKTSubpropertiesToTables;
	private StrabonStatement st;
	private Map<String, String> namespaces;
	private boolean isInitialized;

	public SesameStrabonRepo(String propDictionary, String database, String statFile, String asWKTTablesFile, String sparkAddress, String geoSparkJars, String hadoopHomeDir)
			throws Exception {
		super();
		this.propDictionary=propDictionary;
		this.database=database;
		this.statfile=statFile;
		this.asWKTTablesFile=asWKTTablesFile;
		asWKTSubpropertiesToTables = new HashMap<String, String>();
		namespaces = new HashMap<>();
		this.st = null;
		this.spark = null;
		this.isInitialized = false;
		this.sparkAddress = sparkAddress;
		this.geoSparkJars = geoSparkJars;
		this.hadoopHomeDir = hadoopHomeDir;
	}

	@Override
	public void setDataDir(File file) {

	}

	@Override
	public File getDataDir() {
		return null;
	}

	public void initialize() throws RepositoryException{
		try {
			String jarDirectory = new File(SesameStrabonRepo.class.getProtectionDomain().getCodeSource().getLocation()
					.toURI()).getParentFile().getPath();
			
			log.debug("Setting hadoop.home.dir ---> "+this.hadoopHomeDir);
			System.setProperty("hadoop.home.dir", this.hadoopHomeDir);
			//System.setProperty("spark.sql.warehouse.dir", "/opt/tomcat");
			String[] jars = geoSparkJars.split(",");
			log.debug("Geospark jars will be loaded from "+ jarDirectory );
			log.debug("Geospark jars:" +jars);
			StringBuilder sb = new StringBuilder();
			String comma="";
			for(String jar:jars){
				sb.append(comma);
				sb.append(jarDirectory);
				sb.append("/");
				sb.append(jar);
				comma=",";
			}
			spark = SparkSession.builder()
					//.master("local[*]") // Delete this if run in cluster mode
					.master(this.sparkAddress)
					.appName("strabonQuery") // Change this to a proper name
					// Enable GeoSpark custom Kryo serializer
					.config("spark.serializer", KryoSerializer.class.getName())
					.config("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName())
					.config("spark.sql.inMemoryColumnarStorage.compressed", true)
					.config("hive.exec.dynamic.partition", true).config("spark.sql.parquet.filterPushdown", true)
					//.config("geospark.join.numpartition",2000)
					//.config("spark.default.parallelism", "800")
					//.config("spark.sql.shuffle.partitions", "800")
					.config("spark.driver.memory", "15g")
                                        .config("spark.executor.memory", "65g")
					.config("geospark.join.spatitionside", "none")
					//.config("hadoop.home.dir", "/home/hadoop/SingleNodeYarnSparkHiveHDFSCluster/hadoop")
					.config("spark.jars", sb.toString())
					//.config("spark.jars", "webapps/endpoint2-1.16.1/WEB-INF/lib/geospark*.jar")
					//.config("spark.hadoop.hive.metastore.warehouse.dir", "/opt/tomcat")
					//.config("spark.sql.warehouse.dir", "/opt/tomcat")
					//.config("hive.metastore.warehouse.dir", "/opt/tomcat")
                    			//.config("spark.jars", "/opt/tomcat/webapps/endpoint2-1.16.1/WEB-INF/lib/*.jar")
                    			//.config("spark.jars", "webapps/endpoint2-1.16.1/WEB-INF/lib/geospark-1.3.1.jar,webapps/endpoint2-1.16.1/WEB-INF/lib/geospark-sql_2.3-1.3.1.jar,
						//webapps/endpoint2-1.16.1/WEB-INF/lib/geospark-viz_2.3-1.3.1.jar,webapps/endpoint2-1.16.1/WEB-INF/lib/compress-lzf-1.0.3.jar,
						//webapps/endpoint2-1.16.1/WEB-INF/lib/grizzly-lzma-1.9.46.jar,webapps/endpoint2-1.16.1/WEB-INF/lib/lz4-java-1.4.0.jar")
					//.config("spark.hadoop.fs.default.name", "hdfs://pyravlos3:9001").config("spark.hadoop.fs.defaultFS", "hdfs://pyravlos3:9001")
					.config("spark.sql.inMemoryColumnarStorage.batchSize", 20000).enableHiveSupport().getOrCreate();

			log.debug("Spark session started");

			spark.sql("SET hive.exec.dynamic.partition = true");
			spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict");
			spark.sql("SET hive.exec.max.dynamic.partitions = 4000");
			spark.sql("SET hive.exec.max.dynamic.partitions.pernode = 2000");
			spark.sql("SET spark.sql.inMemoryColumnarStorage.compressed = true");
			spark.sql("SET spark.sql.crossJoin.enabled=true");//for self-spatial joins on geometry table 
			spark.sql("SET spark.sql.parquet.filterPushdown = true");



			//spark.sql("SET spark.sql.hive.metastore.version = 2.3.3");
			//spark.sql("SET spark.sql.warehouse.dir = hdfs://pyravlos3:9001/user/hive/warehouse");
			//spark.sql("SET spark.sql.hive.metastore.jars = /home/hadoop/SingleNodeYarnSparkHiveHDFSCluster/hive/lib/*");
			//spark.sql("SET spark.hadoop.datanucleus.fixedDatastore = true");
			//spark.sql("SET  spark.hadoop.datanucleus.autoCreateSchema =false");


			spark.sql("USE " + database);

			log.debug("Using database "+database);


			GeoSparkSQLRegistrator.registerAll(spark);

			FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());

			try {
				log.debug("Reading othet WKT tables from file: "+ asWKTTablesFile);
				//Path asWKT = new Path(asWKTTablesFile);
				String asWKTFile = LocalQueryTranslator.readFile(asWKTTablesFile);
				for (String nextProp : asWKTFile.split("\n")) {
					asWKTSubpropertiesToTables.put(nextProp, null);
				}
			} catch (Exception fnf) {
				log.error("Could not read other WKT properties file " +fnf.getMessage());

			}
			// TODO Auto-generated method stub
			log.debug("Reading dictionary from file "+propDictionary);
			Map<String, String> predDictionary = LocalQueryTranslator.readPredicates(propDictionary);
			log.debug("property dictionary: "+predDictionary.toString());
			boolean existDefaultGeometrytable = createObdaFile(predDictionary);

			if (existDefaultGeometrytable) {
				// preload geometeries
				log.debug("preloading geometries");
				Dataset<Row> geoms = spark.sql("Select " + StrabonParameters.GEOMETRIES_FIRST_COLUMN + ", "
						+ StrabonParameters.GEOMETRIES_SECOND_COLUMN + ", ST_GeomFromWKT("
						+ StrabonParameters.GEOMETRIES_THIRD_COLUMN + ") as "
						+ StrabonParameters.GEOMETRIES_THIRD_COLUMN + " FROM geometries where "+
						StrabonParameters.GEOMETRIES_THIRD_COLUMN + " IS NOT NULL");
				geoms.createOrReplaceGlobalTempView(StrabonParameters.GEOMETRIES_TABLE);
				geoms.cache();
				long count = geoms.count();
				log.debug("Geometry table "+StrabonParameters.GEOMETRIES_TABLE+" created with "+count+" rows");
			}

			for (String asWKTsubprop : asWKTSubpropertiesToTables.keySet()) {
				String tblName = asWKTSubpropertiesToTables.get(asWKTsubprop);
				log.debug("preloading asWKT subproperty tables: " + asWKTSubpropertiesToTables.toString());
				Dataset<Row> geoms = spark
						.sql("Select s, ST_GeomFromWKT(o) as o FROM " + predDictionary.get(asWKTsubprop) + " ");
				geoms.createOrReplaceGlobalTempView(tblName);
				geoms.cache();
				long count = geoms.count();
				log.debug("Geometry table "+tblName+" created with "+count+" rows");
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
				nse = new NodeSelectivityEstimator(statfile);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			st = reasoner.createStrabonStatement(nse);
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
			isInitialized=true;
		} catch (Exception e1) {
			log.debug("Error: " + e1.getMessage());
			e1.printStackTrace();
			throw new RepositoryException(e1.getMessage());
		}
	}

	@Override
	public boolean isInitialized() {
		return isInitialized;
	}


	public StrabonRepoConnection getConnection() throws RepositoryException{
		StrabonRepoConnection con = null;
		try {
			con = new StrabonRepoConnection(this, spark, st);
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

		if(this.spark ==  null){
			return;
		}
		if(this.st == null){
			try {
				spark.close();
				spark = null;
			} catch (Exception e) {
				e.printStackTrace();
				throw new RepositoryException(e.getMessage());
			}
		}
		else{
			try {
				st.close();
				spark.close();
				spark = null;
				st=null;
			} catch (Exception e) {
				e.printStackTrace();
				throw new RepositoryException(e.getMessage());
			}
		}



		
	}

	public String getType() {
		return QuestConstants.VIRTUAL;
	}

	public ValueFactory getValueFactory() {
		// Gets a ValueFactory for this Repository.
		return ValueFactoryImpl.getInstance();
	}

	public void setNamespace(String key, String value)
	{
		namespaces.put(key, value);
	}

	public String getNamespace(String key)
	{
		return namespaces.get(key);
	}

	public Map<String, String> getNamespaces()
	{
		return namespaces;
	}

	public void setNamespaces(Map<String, String> nsp)
	{
		this.namespaces = nsp;
	}

	public void removeNamespace(String key)
	{
		namespaces.remove(key);
	}

	public static boolean createObdaFile(Map<String, String> predDictionary) throws SQLException, IOException {
		boolean existsGeometryTable = false;
		String schemaPrefix = StrabonParameters.TEMPORARY_SCHEMA_NAME + ".";

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

		int asWKTsubproperty = 0;
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
				obdaFile.append(schemaPrefix + StrabonParameters.GEOMETRIES_TABLE);
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
				obdaFile.append(schemaPrefix + StrabonParameters.GEOMETRIES_TABLE);
				obdaFile.append("\n");
				obdaFile.append("\n");
			} else if (property.contains("has_code") || property.contains("hasDN")) {
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
				obdaFile.append(schemaPrefix + tablename);
				obdaFile.append("\n");
				obdaFile.append("\n");
			} else if (property.contains("hasKey") || property.contains("hasCropTypeName")|| property.contains("hasName")) {
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
			}
			else if (property.contains("hasRECDATE")) {
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
                        }
			 else {
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
