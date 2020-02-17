package inf.unibz.ontop.sesame.tests.general;

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
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.resultio.TupleQueryResultWriterFactory;
//import org.openrdf.query.resultio.sparqlkml.stSPARQLResultsKMLWriter;
//import org.openrdf.query.resultio.sparqlkml.stSPARQLResultsKMLWriterFactory;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.query.resultio.text.tsv.SPARQLResultsTSVWriter;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import it.unibz.krdb.obda.io.ModelIOManager;
import it.unibz.krdb.obda.model.OBDADataFactory;
import it.unibz.krdb.obda.model.OBDAModel;
import it.unibz.krdb.obda.model.impl.OBDADataFactoryImpl;
import it.unibz.krdb.obda.owlrefplatform.core.QuestConstants;
import it.unibz.krdb.obda.owlrefplatform.core.QuestPreferences;
import it.unibz.krdb.obda.owlrefplatform.core.StrabonParameters;
import it.unibz.krdb.obda.owlrefplatform.core.StrabonStatement;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWL;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLConfiguration;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLConnection;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLFactory;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLStatement;
import it.unibz.krdb.sql.ImplicitDBConstraintsReader;
import sesameWrapper.SesameVirtualRepo;

//import org.openrdf.query.resultio.sparqlkml.stSPARQLResultsKMLWriter;

public class QueryExecutor {
	
	static StringBuffer obdaFile;
	static String propDictionary = "/home/dimitris/spatialdbs/dict100m.csv";
	
	public static void main(String[] args) {
		{

			try {

				//String owlfile = "/home/dimitris/spatialdbs/lgd-bremen.owl";
				
				// for opendap its cop.obda
				//String obdafile = "/home/dimitris/spatialdbs/lgd-bremen.obda";
				createObdaFile();
				OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
				OWLOntology ontology = manager.createOntology(); //empty ontology

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
				//p.setCurrentValueOf(QuestPreferences.REFORMULATION_TECHNIQUE, QuestConstants.TW);

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
				try {
					
					
					StrabonStatement st = reasoner.createStrabonStatement();
					String[] query_files = readFilesFromDir("/home/dimitris/spatialdbs/queries/");
					for (String queryfile : query_files) {
						String sparql=readFile(queryfile);
						String sql=st.getUnfolding(sparql);
						System.out.print(sql+"\n");
					}
					
					

					//TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, preds);
					//TupleQueryResultHandler handler = new SPARQLResultsTSVWriter(System.out);

					//tupleQuery.evaluate(handler);

					System.out.println("Closing...");


				} catch (Exception e) {
					e.printStackTrace();
				}

			} catch (Exception e1) {
				e1.printStackTrace();
			}

			System.out.println("Done.");
		}

	}
	
	private static void createObdaFile() throws SQLException {
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
		obdaFile.append("driverClass\tmadgik.exareme.jdbc.embedded.AdpDriver");
		obdaFile.append("\n");

		obdaFile.append("\n");
		obdaFile.append("[MappingDeclaration] @collection [[");
		obdaFile.append("\n");
		
		Map<String, String> predDictionary=readPredicates(propDictionary);
		int mappingId = 0;
		
		for (String property : predDictionary.keySet()) {
			
			
			if(property.contains("asWKT")){
				obdaFile.append("mappingId\tmapp");
				obdaFile.append(mappingId);
				mappingId++;
				obdaFile.append("\n");
				obdaFile.append("target\t");
				obdaFile.append("<{" + StrabonParameters.GEOMETRIES_SECOND_COLUMN +"}> ");
				obdaFile.append("<"+property+">");
				obdaFile.append(" {" + StrabonParameters.GEOMETRIES_THIRD_COLUMN +"}^^geo:wktLiteral .\n");
				obdaFile.append("source\t");
				obdaFile.append("select " + StrabonParameters.GEOMETRIES_SECOND_COLUMN +", "+  StrabonParameters.GEOMETRIES_THIRD_COLUMN +" from ");
				obdaFile.append(StrabonParameters.GEOMETRIES_TABLE);
				obdaFile.append("\n");
				obdaFile.append("\n");
			}
			else if(property.contains("hasGeometry")){
				obdaFile.append("mappingId\tmapp");
				obdaFile.append(mappingId);
				mappingId++;
				obdaFile.append("\n");
				obdaFile.append("target\t");
				obdaFile.append("<{" + StrabonParameters.GEOMETRIES_FIRST_COLUMN +"}> ");
				obdaFile.append("<"+property+">");
				obdaFile.append(" <{" + StrabonParameters.GEOMETRIES_SECOND_COLUMN +"}> .\n");
				obdaFile.append("source\t");
				obdaFile.append("select " + StrabonParameters.GEOMETRIES_FIRST_COLUMN +", "+  StrabonParameters.GEOMETRIES_SECOND_COLUMN +" from ");
				obdaFile.append(StrabonParameters.GEOMETRIES_TABLE);
				obdaFile.append("\n");
				obdaFile.append("\n");
			}
			else if (property.contains("has_code")){
				obdaFile.append("mappingId\tmapp");
				obdaFile.append(mappingId);
				mappingId++;
				obdaFile.append("\n");
				obdaFile.append("target\t");
				obdaFile.append("<{s}> ");
				obdaFile.append("<"+property+">");
				obdaFile.append(" {o}^^xsd:integer .\n");
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
				obdaFile.append("<"+property+">");
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
		Map<String, String> result=new HashMap<String, String>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filename));
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] entry=line.split(",");
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
	public static String readFile(String filename) {
		String file = "";
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filename));
			String line = null;
			while ((line = reader.readLine()) != null) {
				file += line + "\n";
			}
		} catch (Exception e) {
			System.err.println("Error reading file: " + filename);
			e.printStackTrace();
		}
		return file;
	}
}