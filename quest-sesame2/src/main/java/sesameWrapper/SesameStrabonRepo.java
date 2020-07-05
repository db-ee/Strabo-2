package sesameWrapper;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

import it.unibz.krdb.obda.owlrefplatform.core.StrabonStatement;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import it.unibz.krdb.obda.owlrefplatform.core.QuestDBConnection;
import it.unibz.krdb.obda.owlrefplatform.core.QuestPreferences;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWL;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLConfiguration;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLFactory;
import it.unibz.krdb.obda.strabon.QueryExecutor;
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
	private SparkSession spark;
	private static Map<String, String> asWKTSubpropertiesToTables;
	private StrabonStatement st;
	private Map<String, String> namespaces;
	private boolean isInitialized;
	

	public SesameStrabonRepo(String propDictionary, String database, String statFile, String asWKTTablesFile)
			throws Exception {
		super();
		this.propDictionary=propDictionary;
		this.database=database;
		this.statfile=statFile;
		this.asWKTTablesFile=asWKTTablesFile;
		namespaces = new HashMap<>();
		this.st = null;
		this.spark = null;
		this.isInitialized = false;
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

			spark = SparkSession.builder()
					// .master("local[*]") // Delete this if run in cluster mode
					.appName("strabonQuery") // Change this to a proper name
					// Enable GeoSpark custom Kryo serializer
					.config("spark.serializer", KryoSerializer.class.getName())
					.config("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName())
					.config("spark.sql.inMemoryColumnarStorage.compressed", true)
					.config("hive.exec.dynamic.partition", true).config("spark.sql.parquet.filterPushdown", true)
					.config("spark.sql.inMemoryColumnarStorage.batchSize", 20000).enableHiveSupport().getOrCreate();

			spark.sql("SET hive.exec.dynamic.partition = true");
			spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict");
			spark.sql("SET hive.exec.max.dynamic.partitions = 4000");
			spark.sql("SET hive.exec.max.dynamic.partitions.pernode = 2000");
			spark.sql("SET spark.sql.inMemoryColumnarStorage.compressed = true");
			spark.sql("SET spark.sql.crossJoin.enabled=true");//for self-spatial joins on geometry table 
			spark.sql("SET spark.sql.parquet.filterPushdown = true");
			spark.sql("USE " + database);
			GeoSparkSQLRegistrator.registerAll(spark);

			FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());

			try {
				Path asWKT = new Path(asWKTTablesFile);
				String asWKTFile = QueryExecutor.readHadoopFile(asWKT, fs);
				for (String nextProp : asWKTFile.split("\n")) {
					asWKTSubpropertiesToTables.put(nextProp, null);
				}
			} catch (Exception fnf) {
				log.error("Could not read other WKT properties file");

			}
			// TODO Auto-generated method stub
			
			Map<String, String> predDictionary = QueryExecutor.readPredicatesFromHadoop(propDictionary, fs);
			boolean existDefaultGeometrytable = QueryExecutor.createObdaFile(predDictionary);

			if (existDefaultGeometrytable) {
				// preload geometeries
				log.debug("preloading geometries");
				Dataset<Row> geoms = spark.sql("Select " + StrabonParameters.GEOMETRIES_FIRST_COLUMN + ", "
						+ StrabonParameters.GEOMETRIES_SECOND_COLUMN + ", ST_GeomFromWKT("
						+ StrabonParameters.GEOMETRIES_THIRD_COLUMN + ") as "
						+ StrabonParameters.GEOMETRIES_THIRD_COLUMN + " FROM geometries where "+
						StrabonParameters.GEOMETRIES_THIRD_COLUMN + " IS NOT NULL");
				geoms.createOrReplaceGlobalTempView(StrabonParameters.GEOMETRIES_TABLE);
				geoms.count();
				geoms.cache();
			}

			for (String asWKTsubprop : asWKTSubpropertiesToTables.keySet()) {
				String tblName = asWKTSubpropertiesToTables.get(asWKTsubprop);
				log.debug("preloading asWKT subproperty tables");
				Dataset<Row> geoms = spark
						.sql("Select s, ST_GeomFromWKT(o) as o FROM " + predDictionary.get(asWKTsubprop) + " ");
				geoms.createOrReplaceGlobalTempView(tblName);
				geoms.count();
				geoms.cache();
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


}