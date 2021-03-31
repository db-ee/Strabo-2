package madgik.exareme.master.queryProcessor.decomposer.util;

import it.unibz.krdb.obda.model.Function;
import it.unibz.krdb.obda.model.Term;

import java.util.*;

public class VarsToAtoms {
	
	private Set<Term> vars;
	private Set<Function> atoms;
	
	public VarsToAtoms() {
		this.vars = new HashSet<Term>();
		this.atoms = new HashSet<Function>();
	}

	public VarsToAtoms(Set<Term> vars, Set<Function> atoms) {
		super();
		this.vars = vars;
		this.atoms = atoms;
	}

	

	public boolean mergeCommonVar(VarsToAtoms other) {
		//if this VarsToAtoms has common vars with other, it merges
		//other into this and returns true
		//otherwise returns false
		if(Collections.disjoint(this.vars, other.vars)) {
			return false;
		}
		else {
			this.vars.addAll(other.vars);
			this.atoms.addAll(other.atoms);
			return true;
		}
	}

	public List<Term> getVars() {
		List<Term> result=new ArrayList<Term>();
		result.addAll(vars);
		return result;
	}

	public Set<Function> getAtoms() {
		return atoms;
	}

	

}
