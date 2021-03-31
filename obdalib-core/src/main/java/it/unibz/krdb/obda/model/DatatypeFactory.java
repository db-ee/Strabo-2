package it.unibz.krdb.obda.model;

import it.unibz.krdb.obda.model.Predicate.COL_TYPE;
import org.openrdf.model.IRI;

import java.util.List;

public interface DatatypeFactory {

	@Deprecated
	public COL_TYPE getDatatype(String uri);
	
	public COL_TYPE getDatatype(IRI uri);
	
	public IRI getDatatypeIRI(COL_TYPE type);

	
	public DatatypePredicate getTypePredicate(COL_TYPE type);
	
		
	public boolean isBoolean(Predicate p);
	
	public boolean isInteger(Predicate p);
	
	public boolean isFloat(Predicate p);
	
	public boolean isLiteral(Predicate p);
	
	public boolean isString(Predicate p);

	
	
	public List<Predicate> getDatatypePredicates();

}
