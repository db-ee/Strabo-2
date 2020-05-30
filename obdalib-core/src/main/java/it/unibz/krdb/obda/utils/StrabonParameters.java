package it.unibz.krdb.obda.utils;

import it.unibz.krdb.obda.model.BooleanOperationPredicate;
import it.unibz.krdb.obda.model.Function;
import it.unibz.krdb.obda.model.NumericalOperationPredicate;
import it.unibz.krdb.obda.model.Predicate;
import it.unibz.krdb.obda.model.Term;
import it.unibz.krdb.obda.model.impl.OBDAVocabulary;

public class StrabonParameters {
	
	public static final String GEOMETRIES_SCHEMA = "global_temp";
	public static final String GEOMETRIES_TABLE = "geometries2";
	public static final String GEOMETRIES_FIRST_COLUMN = "entity";
	public static final String GEOMETRIES_SECOND_COLUMN = "geom";
	public static final String GEOMETRIES_THIRD_COLUMN = "wkt";
	
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
