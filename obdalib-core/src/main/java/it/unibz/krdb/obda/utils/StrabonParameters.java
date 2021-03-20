package it.unibz.krdb.obda.utils;

import it.unibz.krdb.obda.model.BooleanOperationPredicate;
import it.unibz.krdb.obda.model.Function;
import it.unibz.krdb.obda.model.Predicate;
import it.unibz.krdb.obda.model.Term;
import it.unibz.krdb.obda.model.impl.OBDAVocabulary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StrabonParameters {

	public static final Logger log = LoggerFactory.getLogger(StrabonParameters.class);
	
	public static final String TEMPORARY_SCHEMA_NAME;
	public static final String GEOMETRIES_TABLE ;
	public static final String GEOMETRIES_FIRST_COLUMN;
	public static final String GEOMETRIES_SECOND_COLUMN;
	public static final String GEOMETRIES_THIRD_COLUMN;



	static {
		GenericProperties properties = StrabonProperties.getStrabonProperties();
		//DECOMPOSER_LOG_LEVEL = properties.getString("decomposer.logLevel");
		//Logger.getLogger("madgik.exareme.master.queryProcessor.decomposer")
		//		.setLevel(Level.toLevel(DECOMPOSER_LOG_LEVEL));
		//ANALYZER_LOG_LEVEL = properties.getString("analyzer.logLevel");
		//Logger.getLogger("madgik.exareme.master.queryProcessor.analyzer").setLevel(Level.toLevel(ANALYZER_LOG_LEVEL));
		//Logger.getLogger("madgik.exareme.master.queryProcessor.estimator").setLevel(Level.toLevel(ANALYZER_LOG_LEVEL));

		TEMPORARY_SCHEMA_NAME = properties.getString("temp.schema.name");
		GEOMETRIES_TABLE = properties.getString("geometries.tablename");
		GEOMETRIES_FIRST_COLUMN = properties.getString("geometries.first.column");
		GEOMETRIES_SECOND_COLUMN = properties.getString("geometries.second.column");
		GEOMETRIES_THIRD_COLUMN = properties.getString("geometries.third.column");

		log.trace("Strabon Properties Loaded.");
	}
	
	public static boolean isSpatialJoin(Function atom) {
		Predicate pred = atom.getFunctionSymbol();
		if ( pred.equals(OBDAVocabulary.OVERLAPS) ||
				pred.equals(OBDAVocabulary.SFCONTAINS) || 
				pred.equals(OBDAVocabulary.SFCROSSES ) || 
				pred.equals(OBDAVocabulary.SFDISJOINT ) || 
				pred.equals(OBDAVocabulary.SFINTERSECTS ) || 
				pred.equals(OBDAVocabulary.SFTOUCHES ) || 
				pred.equals(OBDAVocabulary.SFWITHIN ) || 
				pred.equals(OBDAVocabulary.SFEQUALS) || 
				pred.equals(OBDAVocabulary.EHCOVEREDBY) || 
				pred.equals(OBDAVocabulary.EHCOVERS) || 
				pred.equals(OBDAVocabulary.EHDISJOINT) || 
				pred.equals(OBDAVocabulary.EHEQUALS) || 
				pred.equals(OBDAVocabulary.EHINSIDE) || 
				pred.equals(OBDAVocabulary.EHOVERLAPS) || 
				pred.equals(OBDAVocabulary.SFDISTANCE) ||
				pred.equals(OBDAVocabulary.EHCONTAINS) )
			return true;
		else {
			//check if it is arithemtic condition on spatial distance join
			if(pred instanceof BooleanOperationPredicate ) {
				for(Term t:atom.getTerms()) {
					if (t instanceof Function) {
						Function nested=(Function) t;
						if(nested.getFunctionSymbol().equals(OBDAVocabulary.SFDISTANCE))
							return true;
					}
				}
			}
		}
		return false;
	}

}
