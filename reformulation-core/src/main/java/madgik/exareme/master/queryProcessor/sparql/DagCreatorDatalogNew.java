package madgik.exareme.master.queryProcessor.sparql;

import it.unibz.krdb.obda.model.Function;
import it.unibz.krdb.obda.model.*;
import it.unibz.krdb.obda.model.impl.FunctionalTermImpl;
import it.unibz.krdb.obda.model.impl.OBDAVocabulary;
import it.unibz.krdb.obda.utils.StrabonParameters;
import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.dp.DPSubLinear;
import madgik.exareme.master.queryProcessor.decomposer.dp.EquivalentColumnClass;
import madgik.exareme.master.queryProcessor.decomposer.dp.EquivalentColumnClasses;
import madgik.exareme.master.queryProcessor.decomposer.query.Constant;
import madgik.exareme.master.queryProcessor.decomposer.query.*;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;

import java.sql.SQLException;
import java.util.*;

public class DagCreatorDatalogNew {

	private final Map<String, String> predDictionaryStat;
	private CQIE first;
	private NodeHashValues hashes;
	private int alias;
	private int spatialJoinTableNumber;
	private List<Node> tableNodes;
	private List<Table> tables;
	JoinClassMap classes;
	SQLQuery query;
	private List<Integer> filters;
	NodeSelectivityEstimator nse;
	Set<SpatialSelection> spatialSelections;
	Set<SpatialJoin> spatialJoins;
	// list to track filter on base tables, 0 no filter, 1 filter on first, 2
	// filter on second

	public DagCreatorDatalogNew(CQIE q, NodeSelectivityEstimator nse, Map<String, String> predDictionaryStat) {
		super();
		this.first = q;
		this.hashes = hashes;
		classes = new JoinClassMap();
		tableNodes = new ArrayList<Node>();
		tables = new ArrayList<Table>();
		query = new SQLQuery();
		filters = new ArrayList<Integer>();
		this.nse=nse;
		this.predDictionaryStat = predDictionaryStat;
		spatialJoinTableNumber = -1000;
		spatialSelections = new HashSet<>();
		spatialJoins = new HashSet<>();
	}

	public SQLQuery getRootNode() throws SQLException {
		
		
		
		hashes = new NodeHashValues();
		hashes.setSelectivityEstimator(nse);
		
		Node projection = new Node(Node.AND, Node.PROJECT);
		alias = 0;
		// if (pq.getTupleExpr() instanceof Projection) {
		// Projection p = (Projection) pq.getTupleExpr();

		getNodeFromExpression(first);
		EquivalentColumnClasses eqClasses = new EquivalentColumnClasses();
		for (JoinClass jc : classes.getClasses()) {
			if (jc.getColumns().size() > 1) {
				EquivalentColumnClass eqClass = new EquivalentColumnClass(jc.getColumns());
				eqClasses.add(eqClass);
			}
		}

		//add spatial classes
		for(SpatialJoin sj:spatialJoins){
			Set<Column> cols = new HashSet<>();
			cols.add(sj.getCol1());
			cols.add(sj.getCol2());
			EquivalentColumnClass eq = new EquivalentColumnClass(cols);
			eq.setSpatial(true);
			eq.setSpatialJoin(sj.getFunction());
			eqClasses.add(eq);

		}

		DPSubLinear dp = new DPSubLinear(tableNodes, eqClasses);
		dp.setNse(hashes.getNse());
		int tableOrder[] = dp.getPlan();
		List<Function> correctOrder = new ArrayList<>(tables.size());
		for(int i=0; i<tableOrder.length; i++) {
			int tableAtPositionI = tableOrder[i];
			Table t = tables.get(tableAtPositionI);
			for (Function f : first.getBody()) {
				String propNumber = this.predDictionaryStat.get(f.getFunctionSymbol().getName());
				if (propNumber.equals(String.valueOf(t.getName()))) {
					correctOrder.add(f);
					break;
				}

			}
		}
		for(int i=0; i<correctOrder.size(); i++) {
				Function f = correctOrder.get(i);
				first.getBody().remove(f);
				first.getBody().add(i, f);
		}

		eqClasses.renew();
		/*int inserted = 0;
		boolean firstHasNoFilter=true;
		for (int i = 0; i < tableOrder.length; i++) {
			query.addInputTable(tables.get(tableOrder[i]));
			eqClasses.renew();
			boolean hasJoinOnlyInSecond = true;
			for (int j = 0; j < inserted; j++) {
				Set<NonUnaryWhereCondition> joins = eqClasses.getJoin(tableOrder[i] + 1, tableOrder[j] + 1);
				if (joins != null) {
					for (NonUnaryWhereCondition join : joins) {
						query.addBinaryWhereCondition(join);
						if (hasJoinOnlyInSecond && filters.get(tableOrder[i]) == 0) {
							for (Column c : join.getAllColumnRefs()) {
								if (c.getAlias() == tables.get(tableOrder[i]).getAlias() && c.getColumnName()) {
									hasJoinOnlyInSecond = false;
								}
								if(i==1 && firstHasNoFilter){
									//if first table has no filter, choose the replica that sends tuples to the first join sorted
									if (c.getAlias() == tables.get(tableOrder[0]).getAlias() && !c.getColumnName()) {
										tables.get(tableOrder[0]).setInverse(true);
									}
								}
							}
						}
					}
				}
			}
			if (i > 0) {
				if (!hasJoinOnlyInSecond) {
					tables.get(tableOrder[i]).setInverse(false);
				} else {
					if (filters.get(tableOrder[i]) != 1) {
						tables.get(tableOrder[i]).setInverse(true);
					}
				}
			} else {
				if (filters.get(tableOrder[i]) == 2) {
					firstHasNoFilter=false;
					tables.get(tableOrder[i]).setInverse(true);
				}
				if (filters.get(tableOrder[i]) == 1) {
					firstHasNoFilter=false;
				}
			}
			inserted++;
		}

		// projection.addChild(top);
		// Set<Column> projected=new HashSet<Column>();

		madgik.exareme.master.queryProcessor.decomposer.query.Projection prj = new madgik.exareme.master.queryProcessor.decomposer.query.Projection();
		projection.setObject(prj);
		for (Term t : first.getHead().getTerms()) {
			if (t instanceof Variable) {
				Variable varT = (Variable) t;
				Column proj = classes.getFirstColumn(varT.getName());
				// Column proj= new
				// Column(current.getFirstColumn(pe.getSourceName()).getAlias(),
				// pe.getSourceName());
				// projected.add(proj);
				query.getOutputs().add(new Output(varT.getName(), proj));
				// prj.addOperand(new Output(varT.getName(), proj));
			} else {
				System.out.println("what9??? " + t);
			}
		}*/

		// System.out.println(projection.dotPrint(new HashSet<Node>()));
		Node root = new Node(Node.OR);
		root.addChild(projection);
		// System.out.println(query.toSQL());

		return query;
		// Map<String, Set<Column>> eqClasses=new HashMap<String,
		// Set<Column>>();

	}

	private void getNodeForTriplePattern(Function atom, Node top, boolean addToTables, Map<Variable, String> projectedVars) throws SQLException {
		int pred;
		boolean selection = false;
		// Node baseTable=new Node(Node.OR);
		Table predTable = null;
		if (addToTables) {
			filters.add(0);
		}
		Node selNode = new Node(Node.AND, Node.SELECT);
		Selection s = new Selection();
		selNode.setObject(s);
		String subVar = "";
		// JoinClassMap result = new JoinClassMap();
		Term subject = atom.getTerm(0);
		Term object = atom.getTerm(1);
		String predString = null;
		if(predDictionaryStat.containsKey(atom.getFunctionSymbol().getName())){
			predString = predDictionaryStat.get(atom.getFunctionSymbol().getName());
		}
		else {
			predString = atom.getFunctionSymbol().getName();
		}

		//String predString = atom.getFunctionSymbol().getName().replace("prop", "");
		//if(predString.equals(StrabonParameters.GEOMETRIES_TABLE)) {
		//	predString="-1";
		//}
		//if(predString.startsWith("global_temp.tablewkt")) {
		//	predString = predString.replace("global_temp.tablewkt", "-");
		//}

		//if(StrabonParameters.isSpatialFunction(atom)) {
		//	pred = this.spatialJoinTableNumber--;
		//}
		///else {
		pred = Integer.parseInt(predString);
		//}
		Set<Variable> vars = new HashSet<>(2);

		// pred = (int) fetcher.getIdForProperty(predString);

		predTable = new Table(pred, alias);

		// baseTable.setObject(predTable);

		if (subject instanceof Variable) {
			Variable subVarbl = (Variable) subject;
			vars.add(subVarbl);
			String varString = subVarbl.getName();

			// joinCondition.setLeftOp(tablesForVar.iterator().next());
			Column newCol = new Column(alias, true);
			if(addToTables){
				classes.add(varString, newCol);
			} else{ 
				if(projectedVars.get(subject)!=null){
					if(projectedVars.get(subject)=="o"){
						newCol = new Column(alias, false);
					}
					classes.add(varString, newCol);
				}
			
				
			}

			//check if column participates in spatial joins
			for (SpatialJoin sj : spatialJoins) {

				if (subVarbl.equals(sj.getVar1())) {
					sj.setColumn1(newCol);
				}
				if (subVarbl.equals(sj.getVar2())) {
					sj.setColumn2(newCol);
				}

			}
			
			subVar = varString;
			// joinCondition.setRightOp(newCol);
			// tablesForVar.add(newCol);
			// }
			// else{
			// Set<Column> tablesForVar=new HashSet<Column>();
			// tablesForVar.add(new Column(aliasString, "s"));
			// eqClasses.put(varString, tablesForVar);
			// }
		} else if (subject instanceof it.unibz.krdb.obda.model.Constant) {
			it.unibz.krdb.obda.model.Constant con = (it.unibz.krdb.obda.model.Constant) subject;
			createSelection(selNode, selection, con.getValue(), alias, true, addToTables);
			selection = true;
		} else if (subject instanceof FunctionalTermImpl) {
			FunctionalTermImpl fterm = (FunctionalTermImpl) subject;
			if (fterm.getArity() > 1 || !(fterm.getFunctionSymbol().getName().equals("URI"))) {
				System.out.println("what???565 " + subject);
			} else {
				//if (fterm.getTerm(0) instanceof it.unibz.krdb.obda.model.Constant) {
					it.unibz.krdb.obda.model.Constant con = (it.unibz.krdb.obda.model.Constant) fterm.getTerm(0);
					createSelection(selNode, selection, con.getValue(), alias, true, addToTables);
					selection = true;
				//} else {
				//	System.out.println("what???448 " + subject);
				//}
			}
		} else {
			System.out.println("what???8 " + subject);
		}
		if (object instanceof Variable) {
			Variable objVar = (Variable) object;
			vars.add(objVar);
			String varString = objVar.getName();

			if (subVar.equals(varString)) {
				throw new SQLException("same var in subject and object not supported yet");
			}

			Column newCol = new Column(alias, false);
			
			if(addToTables){
				classes.add(varString, newCol);
			} else{ 
				if(projectedVars.get(object)!=null){
					if(projectedVars.get(object)=="s"){
						newCol = new Column(alias, true);
					}
					classes.add(varString, newCol);
				}
			
				
			}

			//check if column participates in spatial joins
			for (SpatialJoin sj : spatialJoins) {

				if (objVar.equals(sj.getVar1())) {
					sj.setColumn1(newCol);
				}
				if (objVar.equals(sj.getVar2())) {
					sj.setColumn2(newCol);
				}

			}

		} else if (object instanceof it.unibz.krdb.obda.model.Constant) {
			it.unibz.krdb.obda.model.Constant con = (it.unibz.krdb.obda.model.Constant) object;
			createSelection(selNode, selection, con.getValue(), alias, false, addToTables);
			selection = true;
		} else if (object instanceof FunctionalTermImpl) {
			FunctionalTermImpl fterm = (FunctionalTermImpl) object;
			
			if (fterm.getArity() > 1) {
				System.out.println("what999 " + object);
			}
			else {
				//else if(fterm.getFunctionSymbol().getName().equals("URI")||fterm.getFunctionSymbol().getName().equals("http://www.w3.org/2000/01/rdf-schema#Literal")) {
				it.unibz.krdb.obda.model.Constant con = (it.unibz.krdb.obda.model.Constant) fterm.getTerm(0);
				createSelection(selNode, selection, con.getValue(), alias, false, addToTables);
				selection = true;
			}
			//} else {
			//	System.out.println("what???448 " + object);
			//}
			
			
			
		} else {
			System.out.println("what???38 " + object);
		}
		if (selection) {
			Node baseNode = new Node(Node.OR);
			baseNode.setObject(predTable);
			hashes.getNse().makeEstimationForNode(baseNode);
			// hashes.put(baseNode.computeHashIDExpand(), baseNode);
			selNode.addChild(baseNode);
			// hashes.put(selNode.computeHashIDExpand(), selNode);
			top.addChild(selNode);
		} else {
			top.setObject(predTable);

		}
		// hashes.put(top.computeHashIDExpand(), top);
		//System.out.println("top::::: "+top);
		hashes.getNse().makeEstimationForNode(top);
		top.addDescendantBaseTable("alias" + alias);
		for(SpatialSelection sel:spatialSelections) {
			if (vars.contains(sel.getVar())) {
				top.getNodeInfo().applySelectivity(sel.getSelectivity());
			}
		}

		if (addToTables) {
			tableNodes.add(top);
			tables.add(predTable);
		}
		// return result;
	}

	private void createSelection(Node selNode, boolean selection, String filter, int aliasString, boolean sOrO,
			boolean addToQuery) throws SQLException {
		// Selection s=null;
		// if(selection){

		// System.out.println(con.getValue());
		Selection s = (Selection) selNode.getObject();
		// }
		// else{
		// selNode=new Node(Node.AND, Node.SELECT);
		// s=new Selection();
		// selNode.setObject(s);
		// }
		if (addToQuery) {
			if (sOrO) {
				filters.set(filters.size() - 1, 1);
			} else {
				filters.set(filters.size() - 1, 2);
			}
		}
		NonUnaryWhereCondition nuwc = new NonUnaryWhereCondition();
		nuwc.setOperator("=");
		nuwc.setLeftOp(new Column(aliasString, sOrO));
		nuwc.setRightOp(new Constant(filter));
		//nuwc.setRightOp(new Constant(fetcher.getIdForUri(filter)));
		s.addOperand(nuwc);
		if (addToQuery) {
			query.addBinaryWhereCondition(nuwc);
		}

	}

	private void getNodeFromExpression(CQIE expr) throws SQLException {
		Set<Function> spatialAtoms = new HashSet<>();
		for (Function atom : expr.getBody()) {
			if(StrabonParameters.isSpatialFunction(atom)) {
				spatialAtoms.add(atom);
			}
		}


		for(Function initialAtom:spatialAtoms) {
			List<Variable> vars = new ArrayList<Variable>(2);
			Function atom = initialAtom;
			//check if it is arithemtic condition on spatial distance join
			Predicate pred = initialAtom.getFunctionSymbol();
			if(pred instanceof BooleanOperationPredicate ) {
				for(Term t:initialAtom.getTerms()) {
					if (t instanceof Function) {
						Function nested=(Function) t;
						if(nested.getFunctionSymbol().equals(OBDAVocabulary.SFDISTANCE))
							atom = nested;
					}
				}
			}

			for(Term t:atom.getTerms()){
				if (t instanceof Variable){
					vars.add((Variable)t);
				}
			}
			if(vars.size()==1){
				SpatialSelection sel = new SpatialSelection(initialAtom, vars.get(0));
				spatialSelections.add(sel);
			}
			else if(vars.size()==2){
				SpatialJoin join = new SpatialJoin(initialAtom, vars.get(0), vars.get(1));
				spatialJoins.add(join);
			}
			expr.getBody().remove(initialAtom);
		}

		for (Function atom : expr.getBody()) {
			String propNumber = null;
			if(predDictionaryStat.containsKey(atom.getFunctionSymbol().getName())) {
				propNumber = "prop" + predDictionaryStat.get(atom.getFunctionSymbol().getName());
			} else {
				propNumber = atom.getFunctionSymbol().getName();
			}
			if (propNumber.startsWith("prop")||propNumber.startsWith(StrabonParameters.GEOMETRIES_TABLE)||propNumber.startsWith("global_temp.tablewkt")) {
				Node top = new Node(Node.OR);
				alias++;
				getNodeForTriplePattern(atom, top, true, null);
			} else {
				//TODO handle filters with greater than, smaller than etc.
				System.out.println("Query cannot be optimized efficiently due to arbitrary expression:" + atom);
			}

		}
		for(SpatialSelection f:spatialSelections){
			expr.getBody().add(f.getFunction());
		}
		for(SpatialJoin f:spatialJoins){
			expr.getBody().add(f.getFunction());
		}

	}

	
	
	

}