package madgik.exareme.master.queryProcessor.decomposer.dp;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import it.unibz.krdb.obda.model.BooleanOperationPredicate;
import it.unibz.krdb.obda.model.Function;
import it.unibz.krdb.obda.model.Predicate;
import it.unibz.krdb.obda.model.Term;
import it.unibz.krdb.obda.model.impl.OBDAVocabulary;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;

public class EquivalentColumnClasses {
	
	private Set<EquivalentColumnClass> classes;

	public EquivalentColumnClasses() {
		super();
		classes=new HashSet<EquivalentColumnClass>();
	}
	
	public void add(EquivalentColumnClass c){
		this.classes.add(c);
	}
	
	public void renew(){
		for(EquivalentColumnClass c:classes){
			c.setUsed(false);
		}
	}

	public Set<NonUnaryWhereCondition> getJoin(int rj, Integer connected) {
		Set<NonUnaryWhereCondition> result=null;
		for(EquivalentColumnClass c:classes){

			if(!c.isUsed()){
				Column right=c.getColumnForTable(rj);
				if(right==null) continue;
				Column left=c.getColumnForTable(connected);
				if(left==null) continue;
				c.setUsed(true);
				if(result==null){
					result=new HashSet<NonUnaryWhereCondition>(1);
				}
				if(c.isSpatial()){
					Predicate pred = c.getSpatialJoin().getFunctionSymbol();
					//check if it is arithemtic condition on spatial distance join
					if(pred instanceof BooleanOperationPredicate) {
						for(Term t:c.getSpatialJoin().getTerms()) {
							if (t instanceof Function) {
								Function nested = (Function) t;
								if (nested.getFunctionSymbol().equals(OBDAVocabulary.SFDISTANCE))
									result.add(new NonUnaryWhereCondition(right, left, nested.getFunctionSymbol().getName()));
								return result;
							}
						}
					}
					result.add(new NonUnaryWhereCondition(right, left, pred.getName()));

				}
				else{
					result.add(new NonUnaryWhereCondition(right, left, "="));
				}
				}


		}
		return result;
	}

	public void computeConnectedTables(Map<Integer, Set<Integer>> connectedTables){
		for(EquivalentColumnClass eqClass:classes){
			eqClass.addConnectedTables(connectedTables);
		}
		
	}

}
