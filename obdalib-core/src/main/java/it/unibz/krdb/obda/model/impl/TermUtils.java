package it.unibz.krdb.obda.model.impl;

import it.unibz.krdb.obda.model.Function;
import it.unibz.krdb.obda.model.Term;
import it.unibz.krdb.obda.model.ValueConstant;
import it.unibz.krdb.obda.model.Variable;

import java.util.Collection;

public class TermUtils {

	public static void addReferencedVariablesTo(Collection<Variable> vars, Function f) {
		for (Term t : f.getTerms()) {
			if (t instanceof Variable) 
				vars.add((Variable)t);
			else if (t instanceof Function)
				addReferencedVariablesTo(vars, (Function)t);
			// else (t instanceof BNode) || (t instanceof URIConstant) || (t instanceof ValueConstant)  
			// no-op
		}
	}
		
	public static void addReferencedVariablesTo(Collection<Variable> vars, Term t) {
		if (t instanceof Function) {
			addReferencedVariablesTo(vars, (Function)t);
		}
		else if (t instanceof Variable) {
			vars.add((Variable)t);
		}
		else /* (t instanceof BNode) || (t instanceof URIConstant) || (t instanceof ValueConstant) */ {
			// no-op
		}
	}
	
	public static void addReferencedVariablesAndURIsTo(Collection<Term> vars, Function f) {
		for (Term t : f.getTerms()) {
			if (t instanceof Variable) 
				vars.add((Variable)t);
			else if (t instanceof ValueConstant) {
				ValueConstant vc=(ValueConstant) t;
				if(vc.getValue().startsWith("http")) {
					//constant IRI
					vars.add(vc);
				}
				
			}
			else if (t instanceof Function)
				addReferencedVariablesAndURIsTo(vars, (Function)t);
			// else (t instanceof BNode) || (t instanceof URIConstant) 
			// no-op
		}
	}
		
	public static void addReferencedVariablesAndURIsTo(Collection<Term> vars, Term t) {
		if (t instanceof Function) {
			addReferencedVariablesAndURIsTo(vars, (Function)t);
		}
		else if (t instanceof ValueConstant) {
			ValueConstant vc=(ValueConstant) t;
			if(vc.getValue().startsWith("http")) {
				//constant IRI
				vars.add(vc);
			}
			
		}
		else if (t instanceof Variable) {
			vars.add((Variable)t);
		}
		else /* (t instanceof BNode) || (t instanceof URIConstant)  */ {
			// no-op
		}
	}
	
}
