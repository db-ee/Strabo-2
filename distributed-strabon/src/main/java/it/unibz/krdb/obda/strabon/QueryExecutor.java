package it.unibz.krdb.obda.strabon;

import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//import org.openrdf.query.resultio.sparqlkml.stSPARQLResultsKMLWriter;

public class QueryExecutor {

	private static final Logger log = LoggerFactory.getLogger(QueryExecutor.class);
	static StringBuffer obdaFile;
	static String propDictionary;
	static String queriesPath;
	static String database;
	static String statfile;
	static String asWKTTablesFile;
	static String splitSpatialJoin;
	private static Map<String, String> asWKTSubpropertiesToTables;

	public static void main(String[] args) throws Exception {
		{
			propDictionary = args[0];
			queriesPath = args[1];
			database = args[2];
			statfile = args[3];
			asWKTTablesFile = args[4];
			splitSpatialJoin = args[5];
			asWKTSubpropertiesToTables = new HashMap<String, String>();
			List<String> tempTables = new ArrayList<String>();
			try {

				final SparkSession spark = SparkSession.builder()
						// .master("local[*]") // Delete this if run in cluster mode
						.appName("strabonQuery") // Change this to a proper name
						// Enable GeoSpark custom Kryo serializer
						.config("spark.serializer", KryoSerializer.class.getName())
						.config("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName())
						//.config("geospark.join.numpartition",2000)
						//.config("spark.default.parallelism", "800")
						//.config("spark.sql.shuffle.partitions", "800")
						.config("geospark.join.spatitionside", "none")
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
					log.debug("Reading file: "+asWKTTablesFile);
					Path asWKT = new Path(asWKTTablesFile);
					String asWKTFile = readHadoopFile(asWKT, fs);
					for (String nextProp : asWKTFile.split("\n")) {
						asWKTSubpropertiesToTables.put(nextProp, null);
					}
				} catch (Exception fnf) {
					log.error("Could not read other WKT properties file "+fnf.getMessage());

				}

				Map<String, String> predDictionary = readPredicatesFromHadoop(propDictionary, fs);
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
					long count=geoms.count();
					log.debug("Geometry table "+StrabonParameters.GEOMETRIES_TABLE+" created with "+count+" rows");

				}

				for (String asWKTsubprop : asWKTSubpropertiesToTables.keySet()) {
					String tblName = asWKTSubpropertiesToTables.get(asWKTsubprop);
					log.debug("preloading asWKT subproperty tables");
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
				StrabonStatement st = reasoner.createStrabonStatement(nse);
				List<String> sparqlQueries = new ArrayList<String>();
				List<String> sqlQueries = new ArrayList<String>();

				Path path = new Path(queriesPath);
				log.debug("reading queries from " + queriesPath);
				if (fs.isDirectory(path)) {
					FileStatus[] fileStatuses = fs.listStatus(path);

					for (FileStatus fileStatus : fileStatuses) {
						if (fileStatus.isFile() && fileStatus.getPath().toString().endsWith(".q")) {
							sparqlQueries.add(readHadoopFile(fileStatus.getPath(), fs));
						}
						if (fileStatus.isFile() && fileStatus.getPath().toString().endsWith(".sql")) {
							sqlQueries.add(readHadoopFile(fileStatus.getPath(), fs));
						}

					}
				}
				String exec="";
				// String[] query_files =
				// readFilesFromDir("/home/dimitris/spatialdbs/queries/");
				for (String sparql : sparqlQueries) {
					try {
						//delete old temp tables
						for (int k = 0; k < tempTables.size(); k++) {
							try{
								spark.sql("DROP VIEW "+StrabonParameters.TEMPORARY_SCHEMA_NAME +"."+tempTables.get(k));
							}
							catch(Exception ex){
								log.error("Could not delete table "+tempTables.get(k)+". ");
								ex.printStackTrace();
							}
						}
						tempTables.clear();
						// String sparql = readFile(queryfile);
						log.debug("Start Executing SPARQL query: "+sparql);
						exec+="Executing SPARQL query: "+sparql +"\n";
						SQLResult sql = st.getUnfolding(sparql, !splitSpatialJoin.equals("false"));
						log.debug("Query unfolded:" + sql.getTextResult() + "\n");
						log.debug("Strating execution");
						long start = System.currentTimeMillis();
						// List<String> tempnames=new ArrayList<String>();
						boolean emptyResult=false;
						for (int k = 0; k < sql.getTempQueries().size(); k++) {
							String temp = sql.getTempQueries().get(k).replaceAll("\"", "");
							log.debug("creating temp table " + sql.getTempName(k) + " with query: " + temp);
							Dataset<Row> tempDataset = spark.sql(temp);
							tempDataset.createOrReplaceGlobalTempView(sql.getTempName(k));
							tempDataset.cache();
							if(tempDataset.isEmpty()){
								log.debug("empty temp query: "+sql.getTempName(k));
								//return empty result
								log.debug("Execution finished in " + (System.currentTimeMillis() - start) + " with 0 results.");
								exec+="Execution finished in " + (System.currentTimeMillis() - start) + " with 0 results.\n";
								emptyResult=true;
								break;
							}
							//long c = tempDataset.count();
							//log.debug("temp table " + sql.getTempName(k) + "had " + c + " results");
							tempTables.add(sql.getTempName(k));
						}
						if(emptyResult){
							continue;
						}
						Dataset<Row> result = spark.sql(sql.getMainQuery().replaceAll("\"", ""));
						//result.cache();
						long resultSize = result.count();
						
						log.debug("Execution finished in " + (System.currentTimeMillis() - start) + " with "
								+ resultSize + " results.");
						exec+="Execution finished in " + (System.currentTimeMillis() - start) + " with "
                                                                + resultSize + " results. \n";
						//result.show(false);
						for (int k = 0; k < sql.getTempQueries().size(); k++) {
							//spark.sql("DROP VIEW globaltemp."+sql.getTempName(k));
						}
					} catch (Exception ex) {
						log.error("Could not execute query " + sparql + "\nException: " + ex.getMessage());
						exec+="Could not execute query " + sparql + "\nException: " + ex.getMessage() +"\n";
						ex.printStackTrace();
					}

				}


				for (String sql : sqlQueries) {
					try {

						log.debug("Start Executing SQL query: "+sql);
						exec+="Executing SPARQL query: "+sql +"\n";
						log.debug("Strating execution");
						long start = System.currentTimeMillis();

						Dataset<Row> result = spark.sql(sql.replaceAll("\"", ""));
						//result.cache();
						long resultSize = result.count();

						log.debug("Execution finished in " + (System.currentTimeMillis() - start) + " with "
								+ resultSize + " results.");
						exec+="Execution finished in " + (System.currentTimeMillis() - start) + " with "
						+ resultSize + " results. \n";

					} catch (Exception ex) {
						log.error("Could not execute query " + sql + "\nException: " + ex.getMessage());
						exec+="Could not execute query " + sql + "\nException: " + ex.getMessage() +"\n";
						ex.printStackTrace();
					}

				}

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
			} else if (property.contains("has_code")) {
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
			} else if (property.contains("hasKey") || property.contains("hasCropTypeName") ) {
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
		} catch(Exception ex) 
        	{ 
            		System.out.println("Exception while reading file:"+ex.getMessage()); 
        	} 
		finally {
			br.close();
		}
		return file;
	}
private static void save_exec(String executionFinished, FileSystem fs) throws IllegalArgumentException, IOException {
                //logger.info("Saving Times");
                //Configuration conf = new Configuration();
                //FileSystem fs = FileSystem.get(conf);
               FSDataOutputStream out = fs.create(new Path(statfile+".exec"));
                        byte[] bytes = executionFinished.getBytes();
                        out.write(bytes, 0, bytes.length);
               
                out.close();

        }

}
