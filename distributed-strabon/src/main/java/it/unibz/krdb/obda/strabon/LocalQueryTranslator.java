package it.unibz.krdb.obda.strabon;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
/*
 * #%L
 * ontop-quest-sesame
 * %%
 * Copyright (C) 2009 - 2013 Free University of Bozen-Bolzano
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unibz.krdb.obda.io.ModelIOManager;
import it.unibz.krdb.obda.model.OBDADataFactory;
import it.unibz.krdb.obda.model.OBDAModel;
import it.unibz.krdb.obda.model.impl.OBDADataFactoryImpl;
import it.unibz.krdb.obda.model.impl.OBDAModelImpl;
import it.unibz.krdb.obda.owlrefplatform.core.QuestConstants;
import it.unibz.krdb.obda.owlrefplatform.core.QuestPreferences;
import it.unibz.krdb.obda.owlrefplatform.core.SQLResult;
import it.unibz.krdb.obda.owlrefplatform.core.StrabonStatement;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWL;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLConfiguration;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLConnection;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLFactory;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLStatement;
import it.unibz.krdb.obda.utils.StrabonParameters;
import it.unibz.krdb.sql.ImplicitDBConstraintsReader;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;

//import org.openrdf.query.resultio.sparqlkml.stSPARQLResultsKMLWriter;

public class LocalQueryTranslator {

	private static final Logger log = LoggerFactory.getLogger(LocalQueryTranslator.class);
	static StringBuffer obdaFile;
	static String propDictionary;
	static String queriesPath;
	static String statfile;
	static String asWKTTablesFile;
	static String dictionaryFile;
	

	private static Map<String, String> dictionary;
	private static Map<String, String> asWKTSubpropertiesToTables;

	public static void main(String[] args) throws Exception {
		{
			propDictionary = args[0];
			queriesPath = args[1];
			statfile = args[2];
			asWKTTablesFile = args[3];
			asWKTSubpropertiesToTables = new HashMap<String, String>();
			dictionaryFile = args[4];

			try {
				String asWKTFile = readFile(asWKTTablesFile);
				for (String nextProp : asWKTFile.split("\n")) {
					asWKTSubpropertiesToTables.put(nextProp, null);
				}
			} catch (Exception fnf) {
				log.error("Could not read other WKT properties file");

			}

			try {

				dictionary = readPredicates(dictionaryFile);

				createObdaFile(readPredicates(propDictionary));
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
				st.setDecoder(dictionary);
				List<String> sparqlQueries = new ArrayList<String>();

				String[] query_files = readFilesFromDir(queriesPath);
				for (String queryfile : query_files) {
					System.out.println("Starting execution of query "+queryfile);
					String sparql = readFile(queryfile);
					SQLResult sql = st.getUnfolding(sparql);
					System.out.println(sql.getTextResult());
				}

				System.out.println("Closing...");

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


		int mappingId = 0;
		int asWKTsubproperty = 0;
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
				obdaFile.append(StrabonParameters.GEOMETRIES_SCHEMA + "." + StrabonParameters.GEOMETRIES_TABLE);
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
				obdaFile.append(StrabonParameters.GEOMETRIES_SCHEMA + "." + tablename);
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
				obdaFile.append(StrabonParameters.GEOMETRIES_SCHEMA + "." + StrabonParameters.GEOMETRIES_TABLE);
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
			} else if (property.contains("hasKey")) {
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

	private static String[] readFilesFromDir(String string) throws IOException {
		File folder = new File(string);
		File[] listOfFiles = folder.listFiles();
		List<String> files = new ArrayList<String>();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile() && listOfFiles[i].getCanonicalPath().endsWith(".q")) {
				files.add(listOfFiles[i].getCanonicalPath());
			}
		}
		java.util.Collections.sort(files);
		return files.toArray(new String[files.size()]);
	}

	public static String readFile(String filename) throws Exception {
		String file = "";
	
			BufferedReader reader = new BufferedReader(new FileReader(filename));
			String line = null;
			while ((line = reader.readLine()) != null) {
				file += line + "\n";
			}
		return file;
	}

}
