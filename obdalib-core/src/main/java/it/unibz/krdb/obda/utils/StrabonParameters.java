package it.unibz.krdb.obda.utils;

import it.unibz.krdb.obda.model.Predicate;
import it.unibz.krdb.obda.model.impl.OBDAVocabulary;

public class StrabonParameters {
	
	public static final String GEOMETRIES_SCHEMA = "global_temp";
	public static final String GEOMETRIES_TABLE = "geometries2";
	public static final String GEOMETRIES_FIRST_COLUMN = "entity";
	public static final String GEOMETRIES_SECOND_COLUMN = "geom";
	public static final String GEOMETRIES_THIRD_COLUMN = "wkt";
	
	public static boolean isSpatialJoin(Predicate pred) {
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
				pred.equals(OBDAVocabulary.EHCONTAINS) )
			return true;
		else
			return false;
	}

}
