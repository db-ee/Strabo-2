package it.unibz.krdb.obda.strabon;

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
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

//import org.openrdf.query.resultio.sparqlkml.stSPARQLResultsKMLWriter;

public class LocalQueryTranslator {

	private static final Logger log = LoggerFactory.getLogger(LocalQueryTranslator.class);
	static StringBuffer obdaFile;
	static String propDictionary;
	static String queriesPath;
	static String statfile;
	static String asWKTTablesFile;
	private static Set<GeometryTable> geometryTables;
	private static Set<String> asWKTproperties;
	
	// the following contain properties that have literls as object for each kind of
	// literal
	// TODO read these from a file
	public static final Set<String> STRINGPROPERTIES = new HashSet<>(Arrays.asList(
			"http://ai.di.uoa.gr/polar/ontology/hasCT", "http://ai.di.uoa.gr/polar/ontology/hasURL",
			"http://ai.di.uoa.gr/polar/ontology/hasThumbnail", "http://ai.di.uoa.gr/polar/ontology/hasCT",
			"http://ai.di.uoa.gr/polar/ontology/hasCTClassName", "http://ai.di.uoa.gr/fs/ontology/hasClassName",
			"http://ai.di.uoa.gr/polar/ontology/hasTitle",
			"http://geographica.di.uoa.gr/generator/landOwnership/hasKey",
			"http://geographica.di.uoa.gr/generator/pointOfInterest/hasKey",
			"http://geographica.di.uoa.gr/generator/state/hasKey", "http://geographica.di.uoa.gr/generator/road/hasKey",
			"http://ai.di.uoa.gr/fs/ontology/hasCropTypeName", "http://ai.di.uoa.gr/eu-hydro/ontology/hasLAN",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasZHYD", "http://ai.di.uoa.gr/eu-hydro/ontology/hasName",
			"http://ai.di.uoa.gr/gadm/ontology/hasName", "http://ai.di.uoa.gr/eu-hydro/ontology/hasCATCH_AREA",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasHYDRONODCT", "http://ai.di.uoa.gr/eu-hydro/ontology/hasREF_TOPO",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasREX", "http://ai.di.uoa.gr/eu-hydro/ontology/hasRN_I_ID",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasDFDD", "http://ai.di.uoa.gr/eu-hydro/ontology/hasSYSTEM_CD",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasEUWFDCODE", "http://ai.di.uoa.gr/eu-hydro/ontology/hasLKE_TYPE",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasNODE_ID", "http://ai.di.uoa.gr/eu-hydro/ontology/hasLAKINOUT",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasNAM", "http://ai.di.uoa.gr/eu-hydro/ontology/hasLAKID",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasBEGLIFEVER", "http://ai.di.uoa.gr/eu-hydro/ontology/hasDRAIN_ID",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasWBodyID", "http://ai.di.uoa.gr/eu-hydro/ontology/hasEU_DAM_ID",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasENDLIFEVER", "http://ai.di.uoa.gr/eu-hydro/ontology/hasTR",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasNEXTUPID", "http://ai.di.uoa.gr/eu-hydro/ontology/hasNODETYPE",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasNEXTDOWNID", "http://ai.di.uoa.gr/fs/ontology/hasNUTS_ID",
			"http://ai.di.uoa.gr/fs/ontology/hasCropTypeName", "http://ai.di.uoa.gr/fs/ontology/hasDescription",
			"http://ai.di.uoa.gr/fs/ontology/hasCNTR_CODE", "http://ai.di.uoa.gr/fs/ontology/hasLC1",
			"http://ai.di.uoa.gr/invekos/ontology/hasCropTypeName"));

	public static final Set<String> INTEGERPROPERTIES = new HashSet<>(Arrays.asList(
			"http://data.linkedeodata.eu/ontology#has_code", "http://ai.di.uoa.gr/eu-hydro/ontology/hasMAINDR_ID",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasSEA_CD", "http://ai.di.uoa.gr/eu-hydro/ontology/hasNVS",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasCOMM_CD", "http://ai.di.uoa.gr/eu-hydro/ontology/hasCCM_ID",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasCGNELIN", "http://ai.di.uoa.gr/eu-hydro/ontology/hasHYP",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasFNODE", "http://ai.di.uoa.gr/eu-hydro/ontology/hasFUN",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasSTRAHLER", "http://ai.di.uoa.gr/eu-hydro/ontology/hasMC",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasPERIMETER", "http://ai.di.uoa.gr/eu-hydro/ontology/hasWMT",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasNUM_SEG", "http://ai.di.uoa.gr/eu-hydro/ontology/hasTNODE",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasWSO_ID", "http://ai.di.uoa.gr/eu-hydro/ontology/hasWCOURSE_ID",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasLOC", "http://ai.di.uoa.gr/eu-hydro/ontology/hasWINDOW",
			"http://ai.di.uoa.gr/fs/ontology/hasLC1_SPEC", "http://ai.di.uoa.gr/fs/ontology/hasCropTypeNumber",
			"http://ai.di.uoa.gr/fs/ontology/hasID", "http://ai.di.uoa.gr/fs/ontology/hasLC1_PERC",
			"http://ai.di.uoa.gr/fs/ontology/hasOBS_DIRECT", "http://ai.di.uoa.gr/fs/ontology/hasRelativeAmount"));

	public static final Set<String> DATETIMEPROPERTIES = new HashSet<>(
			Arrays.asList("http://ai.di.uoa.gr/polar/ontology/hasRECDAT",
					"http://ai.di.uoa.gr/polar/ontology/hasRECDATE", "http://ai.di.uoa.gr/fs/ontology/hasSURVEYDATE",
					"http://ai.di.uoa.gr/fs/ontology/hasEndDate", "http://ai.di.uoa.gr/fs/ontology/hasStartDate"));

	public static final Set<String> DOUBLEPROPERTIES = new HashSet<>(Arrays.asList(
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasCUM_LEN", "http://ai.di.uoa.gr/eu-hydro/ontology/hasELEV",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasLONGPATH", "http://ai.di.uoa.gr/eu-hydro/ontology/hasAREA",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasMAINDR_CLS", "http://ai.di.uoa.gr/eu-hydro/ontology/hasALTITUDE",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasSHAPE_Area", "http://ai.di.uoa.gr/eu-hydro/ontology/hasLEN_TOM",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasDAMX", "http://ai.di.uoa.gr/eu-hydro/ontology/hasDAMY",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasPENTE", "http://ai.di.uoa.gr/eu-hydro/ontology/hasSHAPE_Length",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasLENGTH", "http://ai.di.uoa.gr/fs/ontology/hasArea",
			"http://ai.di.uoa.gr/fs/ontology/hasRelativeAmount", "http://ai.di.uoa.gr/fs/ontology/hasCapabilityValue",
			"http://ai.di.uoa.gr/eu-hydro/ontology/hasPFAFSTETER"));

	public static void main(String[] args) throws Exception {
		{
			propDictionary = args[0];
			queriesPath = args[1];
			statfile = args[2];
			asWKTTablesFile = args[3];
			geometryTables = new HashSet<GeometryTable>();
			asWKTproperties = new HashSet<String>();

			try {
				String asWKTFile = readFile(asWKTTablesFile);
				int geometryTableCounter = 2;
				for (String nextGeometryPair : asWKTFile.split("\n")) {
					//asWKTFile contains pairs of hasGeometry and asWKT subproperties
					String hasGeometrySubproperty = nextGeometryPair.split(",")[0];
					String asWKTubproperty = nextGeometryPair.split(",")[1];
					String tablename = StrabonParameters.GEOMETRIES_TABLE + geometryTableCounter++;
					geometryTables.add(new GeometryTable(tablename, hasGeometrySubproperty, asWKTubproperty));
					asWKTproperties.add(asWKTubproperty);
				}
			} catch (Exception fnf) {
				log.error("Could not read other WKT properties file");

			}

			try {

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
				StrabonStatement st = reasoner.createStrabonStatement(nse);
				Map<String, String> predDictionaryStat = readPredicates(propDictionary + ".stat");
				st.setPredicateDictionaryForStatistics(predDictionaryStat);
				st.setCacheSpatialIndex(false);
				st.setWKTTables(asWKTproperties);
				List<String> sparqlQueries = new ArrayList<String>();

				String[] query_files = readFilesFromDir(queriesPath);
				for (String queryfile : query_files) {
					System.out.println("Starting execution of query " + queryfile);
					String sparql = readFile(queryfile);
					SQLResult sql = st.getUnfolding(sparql, false);
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

	public static void createObdaFile(Map<String, String> predDictionary) throws SQLException, IOException {
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

		for (String property : predDictionary.keySet()) {
			
			//first check to see if it is hasGeometry or as WKT subproperty
			boolean belongsToGeometryTables = false;
			for(GeometryTable geom:geometryTables) {
				if(geom.hasAsWKTSubproperty(property)) {
					belongsToGeometryTables = true;
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
					obdaFile.append(StrabonParameters.TEMPORARY_SCHEMA_NAME + "." + geom.getTablename());
					obdaFile.append("\n");
					obdaFile.append("\n");
				}
				else if (geom.hasGeometrySubproperty(property)) {
					belongsToGeometryTables = true;
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
					obdaFile.append(StrabonParameters.TEMPORARY_SCHEMA_NAME + "." + geom.getTablename());
					obdaFile.append("\n");
					obdaFile.append("\n");
				}
			}
			if (belongsToGeometryTables) {
				continue;
			}
			else if (INTEGERPROPERTIES.contains(property)) {
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
