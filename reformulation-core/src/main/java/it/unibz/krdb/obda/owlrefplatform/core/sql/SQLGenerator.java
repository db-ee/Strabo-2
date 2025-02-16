package it.unibz.krdb.obda.owlrefplatform.core.sql;

/*
 * #%L
 * ontop-reformulation-core
 * %%
 * Copyright (C) 2009 - 2014 Free University of Bozen-Bolzano
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

import it.unibz.krdb.obda.model.*;
import it.unibz.krdb.obda.model.OBDAQueryModifiers.OrderCondition;
import it.unibz.krdb.obda.model.Predicate.COL_TYPE;
import it.unibz.krdb.obda.model.impl.OBDADataFactoryImpl;
import it.unibz.krdb.obda.model.impl.OBDAVocabulary;
import it.unibz.krdb.obda.model.impl.TermUtils;
import it.unibz.krdb.obda.owlrefplatform.core.SQLResult;
import it.unibz.krdb.obda.owlrefplatform.core.basicoperations.DatalogNormalizer;
import it.unibz.krdb.obda.owlrefplatform.core.basicoperations.EQNormalizer;
import it.unibz.krdb.obda.owlrefplatform.core.queryevaluation.SQLDialectAdapter;
import it.unibz.krdb.obda.owlrefplatform.core.srcquerygeneration.SQLQueryGenerator;
import it.unibz.krdb.obda.utils.StrabonParameters;
import it.unibz.krdb.sql.*;
import org.openrdf.model.Literal;

import java.sql.Types;
import java.util.*;
import java.util.regex.Pattern;


public class SQLGenerator implements SQLQueryGenerator {

	private static final long serialVersionUID = 7477161929752147045L;

	/**
	 * Operator symbols
	 */
	private static final String EQ_OPERATOR = "%s = %s";
	private static final String NEQ_OPERATOR = "%s <> %s";
	private static final String GT_OPERATOR = "%s > %s";
	private static final String GTE_OPERATOR = "%s >= %s";
	private static final String LT_OPERATOR = "%s < %s";
	private static final String LTE_OPERATOR = "%s <= %s";
	private static final String AND_OPERATOR = "%s AND %s";
	private static final String OR_OPERATOR = "%s OR %s";
	private static final String NOT_OPERATOR = "NOT %s";
	private static final String IS_NULL_OPERATOR = "%s IS NULL";
	private static final String IS_NOT_NULL_OPERATOR = "%s IS NOT NULL";
	
	/*Spatial operator*/
	
	//TODO this also needs to change because it refers to the PostGIS syntax. Spatial 
	//databases to not necessarily use the same syntax.
	
	//Also,  some of the families use common semantics for their common operators
	//Should be considered in the higher level
	
	private static final String OVERLAPS_OPERATOR = "ST_Overlaps(%s,%s)";
	
	public static final String SFEQUALS_OPERATOR = "ST_Equals(%s,%s)";
	public static final String SFDISJOINT_OPERATOR = "ST_Disjoint(%s,%s)";
	public static final String SFINTERSECTS_OPERATOR = "ST_Intersects(%s,%s)";
	public static final String SFTOUCHES_OPERATOR = "ST_Touches(%s,%s)";
	public static final String SFWITHIN_OPERATOR = "ST_Within(%s,%s)";
	public static final String SFCONTAINS_OPERATOR = "ST_Contains(%s,%s)";
	public static final String SFCROSSES_OPERATOR = "ST_Crosses(%s,%s)";
	public static final String SFDISTANCE_OPERATOR = "ST_Distance(%s,%s,%s)";
	
	public static final String EHEQUALS_OPERATOR = "ST_EQUALS(%s,%s)";
	public static final String EHDISJOINT_OPERATOR = "ST_Disjoint(%s,%s)";
	public static final String EHOVERLAP_OPERATOR = "ST_Overlaps(%s,%s)";
	public static final String EHCOVERS_OPERATOR = "ST_Covers(%s,%s)";
	public static final String EHCOVEREDBY__OPERATOR = "ST_CoveredBy(%s,%s)";
	public static final String EHINSIDE_OPERATOR = "ST_Within(%s,%s)";
	public static final String EHCONTAINS_OPERATOR = "ST_Contains(%s,%s)";
	
	private static final String GEOMFROMWKT_OPERATOR = "ST_GeomFromWKT(%s)";

	private static final String ADD_OPERATOR = "%s + %s";
	private static final String SUBTRACT_OPERATOR = "%s - %s";
	private static final String MULTIPLY_OPERATOR = "%s * %s";
	
	private static final String ABS_OPERATOR = "ABS(%s)";

	private static final String FLOOR_OPERATOR = "FLOOR(%s)";


	private static final String LIKE_OPERATOR = "%s LIKE %s";

	private static final String INDENT = "    ";

	private static final String IS_TRUE_OPERATOR = "%s IS TRUE";

	private static final String typeStrForSELECT = "%s AS %s";
	private static final String typeSuffix = "QuestType";
	private static final String langStrForSELECT = "%s AS %s";
	private static final String langSuffix = "Lang";
	
	/**
	 * Formatting template
	 */
	private static final String VIEW_NAME = "QVIEW%s";
	private static final String VIEW_NAME_PREFIX = "QVIEW";
	private static final String IN_OPERATOR = "%s IN (%s, %s)";

	private final DBMetadata metadata;
	private final SQLDialectAdapter sqladapter;


	private boolean generatingREPLACE = true;
	private boolean distinctResultSet = false;

	private String schemaPrefix;
	
	private final DatatypeFactory dtfac = OBDADataFactoryImpl.getInstance().getDatatypeFactory();
	private boolean useTemporarySchemaName;

	public SQLGenerator(DBMetadata metadata, SQLDialectAdapter sqladapter) {
		this.metadata = metadata;
		this.sqladapter = sqladapter;
		schemaPrefix = "";
		if(useTemporarySchemaName) {
			schemaPrefix = StrabonParameters.TEMPORARY_SCHEMA_NAME + ".";
		}
	}

	/**
	 * 
	 * @param metadata
	 * @param sqladapter
	 * @param sqlGenerateReplace
	 */

	public SQLGenerator(DBMetadata metadata, SQLDialectAdapter sqladapter, boolean sqlGenerateReplace, boolean distinctResultSet) {
		this(metadata, sqladapter);
		this.generatingREPLACE = sqlGenerateReplace;
		this.distinctResultSet = distinctResultSet;

	}


	/**
	 * Generates and SQL query ready to be executed by Quest. Each query is a
	 * SELECT FROM WHERE query. To know more about each of these see the inner
	 * method descriptions.
	 */
	@Override
	public SQLResult generateSourceQuery(DatalogProgram query, List<String> signature) throws OBDAException {
			return generateQuery(query, signature, "");
		
	}

	@Override
	public boolean hasDistinctResultSet() {
		return distinctResultSet;
	}

	@Override
	public void setUseTemporarySchemaName(boolean useTemporarySchemaName) {
		this.useTemporarySchemaName = useTemporarySchemaName;
		if(useTemporarySchemaName){
			schemaPrefix = StrabonParameters.TEMPORARY_SCHEMA_NAME + ".";
		}
	}

	private boolean hasSelectDistinctStatement(CQIE cq) {
		boolean toReturn = false;
		if (cq.getQueryModifiers().hasModifiers()) {
			toReturn = cq.getQueryModifiers().isDistinct();
		}
		return toReturn;
	}
	
	private boolean hasOrderByClause(CQIE query) {
		boolean toReturn = false;
		if (query.getQueryModifiers().hasModifiers()) {
			final List<OrderCondition> conditions = query.getQueryModifiers().getSortConditions();
			toReturn = (conditions.isEmpty()) ? false : true;
		}
		return toReturn;
	}

	/**
	 * Main method. Generates the full query, taking into account
	 * limit/offset/order by.
	 */
	private SQLResult generateQuery(DatalogProgram query, List<String> signature,
			String indent) throws OBDAException {

		int numberOfQueries = query.getRules().size();

		List<String> queriesStrings = new LinkedList<String>();
		/* Main loop, constructing the SPJ query for each CQ */
		List<String> tempResults = new ArrayList<String>();
		List<String> tempNames = new ArrayList<String>();
		boolean outerDistinct=false;
		int queryPos = 0;
		for (CQIE cq : query.getRules()) {
			queryPos++;
			boolean isDistinct = hasSelectDistinctStatement(cq);
			/*
			 * Here we normalize so that the form of the CQ is as close to the
			 * form of a normal SQL algebra as possible, particularly, no shared
			 * variables, only joins by means of equality. Also, equalities in
			 * nested expressions (JOINS) are kept at their respective levels to
			 * generate correct ON and wHERE clauses.
			 */
//			log.debug("Before pushing equalities: \n{}", cq);

			EQNormalizer.enforceEqualities(cq);

//			log.debug("Before folding Joins: \n{}", cq);

			DatalogNormalizer.foldJoinTrees(cq);

//			log.debug("Before pulling out equalities: \n{}", cq);
			
			DatalogNormalizer.pullOutEqualities(cq);
			
//			log.debug("Before pulling out Left Join Conditions: \n{}", cq);
			
			DatalogNormalizer.pullOutLeftJoinConditions(cq);
			
//			log.debug("Before pulling up nested references: \n{}", cq);

			DatalogNormalizer.pullUpNestedReferences(cq);

//			log.debug("Before adding trivial equalities: \n{}, cq);", cq);

			DatalogNormalizer.addMinimalEqualityToLeftJoin(cq);

//			log.debug("Normalized CQ: \n{}", cq);

			Predicate headPredicate = cq.getHead().getFunctionSymbol();
			boolean isTempView = headPredicate.getName().toString().startsWith(OBDAVocabulary.TEMP_VIEW_QUERY);

			if (!(headPredicate.getName().toString().equals(OBDAVocabulary.QUEST_QUERY)
					|| isTempView)) {
				// not a target query, skip it.
				continue;
			}

			QueryAliasIndex index = new QueryAliasIndex(cq);
			
			boolean innerdistincts = false;
			if (isDistinct && !distinctResultSet && numberOfQueries == queryPos) {
				innerdistincts = true;
			}

			String FROM = getFROM(cq, index);
			String WHERE = getWHERE(cq, index);
			String SELECT = getSelectClause(signature, cq, index, innerdistincts, !isTempView);

			String querystr = SELECT + FROM + WHERE;
			
			
			
			boolean isOrderBy = hasOrderByClause(cq);
			if (cq.getQueryModifiers().hasModifiers()) {
				//String indent = "   ";
				String outerViewName = "SUB_QVIEW";
				
				

				String modifier = "";
				List<OrderCondition> conditions = cq.getQueryModifiers().getSortConditions();
				long limit = cq.getQueryModifiers().getLimit();
				long offset = cq.getQueryModifiers().getOffset();
				modifier = sqladapter.sqlOrderByAndSlice(conditions,outerViewName,limit, offset) + "\n";

				String sql = "SELECT *\n";
				sql += "FROM (\n";
				sql += querystr + "\n";
				sql += ") " + outerViewName + "\n";
				sql += modifier;
				querystr = sql;
			}
			
			
			if(isTempView) {
				tempResults.add(querystr);
				tempNames.add(headPredicate.getName().toString());
			}
			else {
				outerDistinct=isDistinct;
				queriesStrings.add(querystr);
			}
			
		}

		Iterator<String> queryStringIterator = queriesStrings.iterator();
		StringBuilder result = new StringBuilder();
		if (queryStringIterator.hasNext()) {
			result.append(queryStringIterator.next());
		}

		String UNION = null;
		if (outerDistinct && !distinctResultSet) {
			UNION = "UNION";
		} else {
			UNION = "UNION ALL";
		}
		while (queryStringIterator.hasNext()) {
			result.append("\n");
			result.append(UNION);
			result.append("\n");
			result.append(queryStringIterator.next());
		}

		return new SQLResult(result.toString(), tempResults, tempNames, signature);
	}

	/***
	 * Returns a string with boolean conditions formed with the boolean atoms
	 * found in the atoms list.
	 */
	private LinkedHashSet<String> getBooleanConditionsString(List<Function> atoms, QueryAliasIndex index) {
		LinkedHashSet<String> conditions = new LinkedHashSet<String>();
		for (int atomidx = 0; atomidx < atoms.size(); atomidx++) {
			Term innerAtom = atoms.get(atomidx);
			Function innerAtomAsFunction = (Function) innerAtom;
			if (innerAtomAsFunction.isBooleanFunction()) {
				String condition = getSQLCondition(innerAtomAsFunction, index);
				conditions.add(condition);
			}else if (innerAtomAsFunction.isDataTypeFunction()) {
				String condition = getSQLString(innerAtom, index, false);
				conditions.add(condition);
			}
		}
		return conditions;
	}

	/***
	 * Returns the SQL for an atom representing an SQL condition (booleans).
	 */
	private String getSQLCondition(Function atom, QueryAliasIndex index) {
		Predicate functionSymbol = atom.getFunctionSymbol();
		if (isUnary(atom)) {
			if (atom.isArithmeticFunction()) {
				String expressionFormat = getNumericalOperatorString(functionSymbol);
				Term term = atom.getTerm(0);
				String column = getSQLString(term, index, false);
				return String.format(expressionFormat, column);
			}
			// For unary boolean operators, e.g., NOT, IS NULL, IS NOT NULL.
			// added also for IS TRUE
			String expressionFormat = getBooleanOperatorString(functionSymbol);
			Term term = atom.getTerm(0);
			String column = getSQLString(term, index, false);
			if (expressionFormat.contains("NOT %s") ) {
				// find data type of term and evaluate accordingly
				//int type = 8;
				if (term instanceof Function) {
					Function f = (Function) term;
					if (!f.isDataTypeFunction()) return String.format(expressionFormat, column);
				}
				int type = getVariableDataType(term);
				if (type == Types.INTEGER) return String.format("NOT %s > 0", column);
                if (type == Types.BIGINT) return String.format("NOT %s > 0", column);
                if (type == Types.FLOAT) return String.format("NOT %s > 0", column);
				if (type == Types.DOUBLE) return String.format("NOT %s > 0", column);
				if (type == Types.BOOLEAN) return String.format("NOT %s", column);
				if (type == Types.VARCHAR) return String.format("NOT LENGTH(%s) > 0", column);
				return "0;";
			}
			if (expressionFormat.contains("IS TRUE")) {
				// find data type of term and evaluate accordingly
				//int type = 8;
				int type = getVariableDataType(term);
				if (type == Types.INTEGER) return String.format("%s > 0", column);
                if (type == Types.BIGINT) return String.format("%s > 0", column);
                if (type == Types.FLOAT) return String.format("%s > 0", column);
				if (type == Types.DOUBLE) return String.format("%s > 0", column);
				if (type == Types.BOOLEAN) return String.format("%s", column);
				if (type == Types.VARCHAR) return String.format("LENGTH(%s) > 0", column);
				return "1;";
			}
			return String.format(expressionFormat, column);
		} else if (isBinary(atom)) {
			if (atom.isBooleanFunction()) {
				if(functionSymbol.equals(OBDAVocabulary.SPARQL_IN)){
					StringBuffer sb=new StringBuffer();
					Term first = atom.getTerm(0);
					sb.append(getSQLString(first, index, false));
					sb.append(" IN (");
					String comma="";
					for(int i=1;i<atom.getTerms().size();i++){
						sb.append(comma);
						sb.append(getSQLString(atom.getTerm(i), index, false));
						comma=", ";
					}
					sb.append(")");
					return sb.toString();
				}
				// For binary boolean operators, e.g., AND, OR, EQ, GT, LT, etc. _
				String expressionFormat = getBooleanOperatorString(functionSymbol);
				Term left = atom.getTerm(0);
				Term right = atom.getTerm(1);
				String leftOp = getSQLString(left, index, true);
				String rightOp = getSQLString(right, index, true);
				if(atom.isSpatialRelationFunction()) {
					//if one of the two operands is a geometry literal do not search among cached geometries
					if( !atom.toString().contains("GEOMFROMWKT")) {
						return String.format("(" + expressionFormat + ")", leftOp, rightOp);	
					}
				}
				return String.format("(" + expressionFormat + ")", leftOp, rightOp);
			} else if (atom.isArithmeticFunction()) {
				// For numerical operators, e.g., MULTIPLY, SUBTRACT, ADDITION
				String expressionFormat = getNumericalOperatorString(functionSymbol);
				Term left = atom.getTerm(0);
				Term right = atom.getTerm(1);
				String leftOp = getSQLString(left, index, true);
				String rightOp = getSQLString(right, index, true);
				return String.format("(" + expressionFormat + ")", leftOp, rightOp);
			} else {
				throw new RuntimeException("The binary function " 
						+ functionSymbol.toString() + " is not supported yet!");
			}
		} else {
			if (functionSymbol == OBDAVocabulary.SPARQL_REGEX) {
				boolean caseinSensitive = false;
				boolean multiLine = false;
				boolean dotAllMode = false;
				if (atom.getArity() == 3) {
					if (atom.getTerm(2).toString().contains("i")) {
						caseinSensitive = true;
					}
					if (atom.getTerm(2).toString().contains("m")) {
						multiLine = true;
					}
					if (atom.getTerm(2).toString().contains("s")) {
						dotAllMode = true;
					}
				}
				Term p1 = atom.getTerm(0);
				Term p2 = atom.getTerm(1);
				
				String column = getSQLString(p1, index, false);
				String pattern = getSQLString(p2, index, false);
				return sqladapter.sqlRegex(column, pattern, caseinSensitive, multiLine, dotAllMode);
			} else {
				throw new RuntimeException("The builtin function "
						+ functionSymbol.toString() + " is not supported yet!");
			}
		}
	}

	private String getTblde9imColumn(String function) {
		if(function.toLowerCase().contains("contains")) {
			return "contains";
		}
		else if(function.toLowerCase().contains("coveredby")) {
			return "coveredby";
		}
		else if(function.toLowerCase().contains("covers")) {
			return "covers";
		}
		else if(function.toLowerCase().contains("crosses")) {
			return "crosses";
		}
		else if(function.toLowerCase().contains("equals")) {
			return "equals";
		}
		else if(function.toLowerCase().contains("intersects")) {
			return "intersects";
		}
		else if(function.toLowerCase().contains("overlaps")) {
			return "overlaps";
		}
		else if(function.toLowerCase().contains("touches")) {
			return "touches";
		}
		else if(function.toLowerCase().contains("within")) {
			return "within";
		}
		else {
			return "";
		}
	}

	/**
	 * Returns the table definition for these atoms. By default, a list of atoms
	 * represents JOIN or LEFT JOIN of all the atoms, left to right. All boolean
	 * atoms in the list are considered conditions in the ON clause of the JOIN.
	 * 
	 * <p>
	 * If the list is a LeftJoin, then it can only have 2 data atoms, and it HAS
	 * to have 2 data atoms.
	 * 
	 * <p>
	 * If process boolean operators is enabled, all boolean conditions will be
	 * added to the ON clause of the first JOIN.
	 * 
	 * @param inneratoms
	 * @param index
	 * @param isTopLevel
	 *            indicates if the list of atoms is actually the main body of
	 *            the conjunctive query. If it is, no JOIN is generated, but a
	 *            cross product with WHERE clause. Moreover, the isLeftJoin
	 *            argument will be ignored.
	 * 
	 * @return
	 */
	private String getTableDefinitions(List<Function> inneratoms,
			QueryAliasIndex index, boolean isTopLevel, boolean isLeftJoin,
			String indent) {
		/*
		 * We now collect the view definitions for each data atom each
		 * condition, and each each nested Join/LeftJoin
		 */
		List<String> tableDefinitions = new LinkedList<String>();
		for (int atomidx = 0; atomidx < inneratoms.size(); atomidx++) {
			Term innerAtom = inneratoms.get(atomidx);
			Function innerAtomAsFunction = (Function) innerAtom;
			String definition = getTableDefinition(innerAtomAsFunction, index, indent + INDENT);
			if (!definition.isEmpty()) {
				tableDefinitions.add(definition);
			}
		}

		/*
		 * Now we generate the table definition, this will be either a comma
		 * separated list for TOP level (FROM clause) or a Join/LeftJoin
		 * (possibly nested if there are more than 2 table definitions in the
		 * current list) in case this method was called recursively.
		 */
		StringBuilder tableDefinitionsString = new StringBuilder();

		int size = tableDefinitions.size();
		if (isTopLevel) {
			if (size == 0) {
				tableDefinitionsString.append("(" + sqladapter.getDummyTable() + ") tdummy ");
				
			} else {
			Iterator<String> tableDefinitionsIterator = tableDefinitions.iterator();
			tableDefinitionsString.append(indent);
			tableDefinitionsString.append(tableDefinitionsIterator.next());
			while (tableDefinitionsIterator.hasNext()) {
				tableDefinitionsString.append(",\n");
				tableDefinitionsString.append(indent);
				tableDefinitionsString.append(tableDefinitionsIterator.next());
			}
			}
		} else {
			/*
			 * This is actually a Join or LeftJoin, so we form the JOINs/LEFT
			 * JOINs and the ON clauses
			 */
			String JOIN_KEYWORD = null;
			if (isLeftJoin) {
				JOIN_KEYWORD = "LEFT OUTER JOIN";
			} else {
				JOIN_KEYWORD = "JOIN";
			}
			String JOIN = "\n" + indent + "(\n" + indent + "%s\n" + indent
					+ JOIN_KEYWORD + "\n" + indent + "%s\n" + indent + ")";

			if (size == 0) {
				throw new RuntimeException("Cannot generate definition for empty data");
			}
			if (size == 1) {
				return tableDefinitions.get(0);
			}

			/*
			 * To form the JOIN we will cycle through each data definition,
			 * nesting the JOINs as we go. The conditions in the ON clause will
			 * go on the TOP level only.
			 */
			String currentJoin = String.format(JOIN,
					tableDefinitions.get(size - 2),
					tableDefinitions.get(size - 1));
			tableDefinitions.remove(size - 1);
			tableDefinitions.remove(size - 2);

			int currentSize = tableDefinitions.size();
			while (currentSize > 0) {
				currentJoin = String.format(JOIN, tableDefinitions.get(currentSize - 1), currentJoin);
				tableDefinitions.remove(currentSize - 1);
				currentSize = tableDefinitions.size();
			}
			tableDefinitions.add(currentJoin);

			tableDefinitionsString.append(currentJoin);
			/*
			 * If there are ON conditions we add them now. We need to remove the
			 * last parenthesis ')' and replace it with ' ON %s)' where %s are
			 * all the conditions
			 */
			String conditions = getConditionsString(inneratoms, index, true, indent);

			if (conditions.length() > 0 && tableDefinitionsString.lastIndexOf(")") != -1) {
				int lastidx = tableDefinitionsString.lastIndexOf(")");
				tableDefinitionsString.delete(lastidx, tableDefinitionsString.length());
				String ON_CLAUSE = String.format("ON\n%s\n " + indent + ")", conditions);
				tableDefinitionsString.append(ON_CLAUSE);
			}
		}
		return tableDefinitionsString.toString();
	}

	/**
	 * Returns the table definition for the given atom. If the atom is a simple
	 * table or view, then it returns the value as defined by the
	 * QueryAliasIndex. If the atom is a Join or Left Join, it will call
	 * getTableDefinitions on the nested term list.
	 */
	private String getTableDefinition(Function atom, QueryAliasIndex index, String indent) {
		Predicate predicate = atom.getFunctionSymbol();
		if ((predicate instanceof BooleanOperationPredicate && !predicate.isSpatialRelationPredicate())
				|| predicate instanceof NumericalOperationPredicate
				|| predicate instanceof DatatypePredicate) {
			// These don't participate in the FROM clause
			return "";
		} else if (predicate instanceof AlgebraOperatorPredicate) {
			List<Function> innerTerms = new LinkedList<Function>();
			for (Term innerTerm : atom.getTerms()) {
				innerTerms.add((Function) innerTerm);
			}
			if (predicate == OBDAVocabulary.SPARQL_JOIN) {
				return getTableDefinitions(innerTerms, index, false, false, indent + INDENT);
			} else if (predicate == OBDAVocabulary.SPARQL_LEFTJOIN) {
				return getTableDefinitions(innerTerms, index, false, true, indent + INDENT);
			}
		}
		else if(predicate.isSpatialRelationPredicate()) {
			return "";
		}

		/*
		 * This is a data atom
		 */
		String def = index.getViewDefinition(atom);
		return def;
	}

	private String getFROM(CQIE query, QueryAliasIndex index) {
		List<Function> atoms = new LinkedList<Function>();
		for (Function atom : query.getBody()) {
			atoms.add((Function) atom);
		}
		String tableDefinitions = getTableDefinitions(atoms, index, true, false, "");
		return "\n FROM \n" + tableDefinitions;
	}

	/**
	 * Generates all the conditions on the given atoms, e.g., shared variables
	 * and boolean conditions. This string can then be used to form a WHERE or
	 * an ON clause.
	 * 
	 * <p>
	 * The method assumes that no variable in this list (or nested ones) referes
	 * to an upper level one.
	 */
	private String getConditionsString(List<Function> atoms,
			QueryAliasIndex index, boolean processShared, String indent) {

		LinkedHashSet<String> equalityConditions = new LinkedHashSet<String>();

		// if (processShared)
		equalityConditions.addAll(getConditionsSharedVariablesAndConstants(atoms, index, processShared));
		LinkedHashSet<String> booleanConditions = getBooleanConditionsString(atoms, index);

		LinkedHashSet<String> conditions = new LinkedHashSet<String>();
		conditions.addAll(equalityConditions);
		conditions.addAll(booleanConditions);

		/*
		 * Collecting all the conditions in a single string for the ON or WHERE
		 * clause
		 */
		StringBuilder conditionsString = new StringBuilder();
		Iterator<String> conditionsIterator = conditions.iterator();
		if (conditionsIterator.hasNext()) {
			conditionsString.append(indent);
			conditionsString.append(conditionsIterator.next());
		}
		while (conditionsIterator.hasNext()) {
			conditionsString.append(" AND\n");
			conditionsString.append(indent);
			conditionsString.append(conditionsIterator.next());
		}
		return conditionsString.toString();
	}

	/**
	 * Returns the set of variables that participate data atoms (either in this
	 * atom directly or in nested ones). This will recursively collect the
	 * variables references in in this atom, exlcuding those on the right side
	 * of left joins.
	 * 
	 * @param atom
	 * @return
	 */
	private Set<Variable> getVariableReferencesWithLeftJoin(Function atom) {
		
		if (atom.isDataFunction()) {
			Set<Variable> variables = new LinkedHashSet<>();
			TermUtils.addReferencedVariablesTo(variables, atom);
			return variables;
		}
		else if (atom.isBooleanFunction()) {
			return Collections.emptySet();
		}
		else if (atom.isDataTypeFunction()) {
			return Collections.emptySet();
		}
		/*
		 * we have an alebra opertaor (join or left join) if its a join, we need
		 * to collect all the varaibles of each nested atom., if its a left
		 * join, only of the first data/algebra atom (the left atom).
		 */
		boolean isLeftJoin = false;
		boolean foundFirstDataAtom = false;

		if (atom.getFunctionSymbol() == OBDAVocabulary.SPARQL_LEFTJOIN) {
			isLeftJoin = true;
		}
		LinkedHashSet<Variable> innerVariables = new LinkedHashSet<Variable>();
		for (Term t : atom.getTerms()) {
			if (isLeftJoin && foundFirstDataAtom) {
				break;
			}
			Function asFunction = (Function) t;
			if (asFunction.isBooleanFunction()) {
				continue;
			}
			innerVariables.addAll(getVariableReferencesWithLeftJoin(asFunction));
			foundFirstDataAtom = true;
		}
		return innerVariables;

	}

	/**
	 * Returns a list of equality conditions that reflect the semantics of the
	 * shared variables in the list of atoms.
	 * <p>
	 * The method assumes that no variables are shared across deeper levels of
	 * nesting (through Join or LeftJoin atoms), it will not call itself
	 * recursively. Nor across upper levels.
	 * 
	 * <p>
	 * When generating equalities recursively, we will also generate a minimal
	 * number of equalities. E.g., if we have A(x), Join(R(x,y), Join(R(y,
	 * x),B(x))
	 * 
	 */
	private LinkedHashSet<String> getConditionsSharedVariablesAndConstants(
			List<Function> atoms, QueryAliasIndex index, boolean processShared) {
		LinkedHashSet<String> equalities = new LinkedHashSet<String>();

		Set<Variable> currentLevelVariables = new LinkedHashSet<Variable>();
		if (processShared) {
			for (Function atom : atoms) {
				currentLevelVariables.addAll(getVariableReferencesWithLeftJoin(atom));
			}
		}
		
		/*
		 * For each variable we collect all the columns that should be equated
		 * (due to repeated positions of the variable). then we form atoms of
		 * the form "COL1 = COL2"
		 */
		for (Variable var : currentLevelVariables) {
			Set<QualifiedAttributeID> references = index.getColumnReferences(var);
			if (references.size() < 2) {
				// No need for equality
				continue;
			}
			Iterator<QualifiedAttributeID> referenceIterator = references.iterator();
			QualifiedAttributeID leftColumnReference = referenceIterator.next();
			while (referenceIterator.hasNext()) {
				QualifiedAttributeID rightColumnReference = referenceIterator.next();
				String leftColumnString = leftColumnReference.getSQLRendering();
				String rightColumnString = rightColumnReference.getSQLRendering();
				String equality = String.format("(%s = %s)", leftColumnString, rightColumnString);
				equalities.add(equality);
				leftColumnReference = rightColumnReference;
			}
		}

		for (Function atom : atoms) {
			if (!atom.isDataFunction()) {
				continue;
			}
			for (int idx = 0; idx < atom.getArity(); idx++) {
				Term l = atom.getTerm(idx);
				if (l instanceof Constant) {
					String value = getSQLString(l, index, false);
					String columnReference = index.getColumnReference(atom, idx);
					equalities.add(String.format("(%s = %s)", columnReference, value));
				}
			}

		}
		return equalities;
	}

	
	// return variable SQL data type
	private int getVariableDataType (Term term) {
		Function f = (Function) term;
		if (f.isDataTypeFunction()) {
			Predicate p = f.getFunctionSymbol();
			Predicate.COL_TYPE type = dtfac.getDatatype(p.toString());
			return OBDADataFactoryImpl.getInstance().getJdbcTypeMapper().getSQLType(type);
		}
		// Return varchar for unknown
		return Types.VARCHAR;
	}

	private String getWHERE(CQIE query, QueryAliasIndex index) {
		List<Function> atoms = new LinkedList<Function>();
		for (Function atom : query.getBody()) {
			atoms.add((Function) atom);
		}
		String conditions = getConditionsString(atoms, index, false, "");
		if (conditions.length() == 0) {
			return "";
		}
		return "\nWHERE \n" + conditions;
	}

	/**
	 * produces the select clause of the sql query for the given CQIE
	 * 
	 * @param query
	 *            the query
	 * @return the sql select clause
	 */
	private String getSelectClause(List<String> signature, CQIE query,
			QueryAliasIndex index, boolean distinct, boolean transformGeoToWKT) throws OBDAException {
		/*
		 * If the head has size 0 this is a boolean query.
		 */
		if(query.getSignature()!=null) {
			//overwrite global signature
			signature=query.getSignature();
		}
		List<Term> headterms = query.getHead().getTerms();
		StringBuilder sb = new StringBuilder();

		sb.append("SELECT ");
		if (distinct && !distinctResultSet) {
			sb.append("DISTINCT ");
		}
		//Only for ASK
		if (headterms.size() == 0) {
			sb.append("'true' as x");
			return sb.toString();
		}

		/**
		 * Set that contains all the variable names created on the top query.
		 * It helps the dialect adapter to generate variable names according to its possible restrictions.
		 * Currently, this is needed for the Oracle adapter (max. length of 30 characters).
		 */
		Set<String> sqlVariableNames = new HashSet<>();

		Iterator<Term> hit = headterms.iterator();
		int hpos = 0;
		while (hit.hasNext()) {

			Term ht = hit.next();
			//String typeColumn = getTypeColumnForSELECT(ht, signature, hpos, sqlVariableNames);
			//String langColumn = getLangColumnForSELECT(ht, signature, hpos,	index, sqlVariableNames);
			String mainColumn = getMainColumnForSELECT(ht, signature, hpos, index, sqlVariableNames, transformGeoToWKT);

			sb.append("\n   ");
			//sb.append(typeColumn);
			//sb.append(", ");
			//sb.append(langColumn);
			//sb.append(", ");
			sb.append(mainColumn);
			if (hit.hasNext()) {
				sb.append(", ");
			}
			hpos++;
		}
		return sb.toString();
	}

	private String getMainColumnForSELECT(Term ht,
			List<String> signature, int hpos, QueryAliasIndex index, Set<String> sqlVariableNames, boolean transformGeoToWKT) {

		/**
		 * Creates a variable name that fits to the restrictions of the SQL dialect.
		 */
		String variableName = sqladapter.nameTopVariable(signature.get(hpos), "", sqlVariableNames);
		sqlVariableNames.add(variableName);

		String mainColumn = null;

		String mainTemplate = "%s AS %s";

		if (ht instanceof URIConstant) {
			URIConstant uc = (URIConstant) ht;
			mainColumn = sqladapter.getSQLLexicalFormString(uc.getIRI().toString());
		} else if (ht == OBDAVocabulary.NULL) {
			mainColumn = "NULL";
		}
		else if (ht instanceof Variable) {
			Variable v = (Variable) ht;
			mainColumn = sqladapter.sqlCast(getSQLString(ht, index, false), Types.VARCHAR);
			if(transformGeoToWKT && isGeomColType(v, index)){
				mainColumn = "ST_AsText(" + mainColumn + ")";
				System.out.println("main col: " + mainColumn);
			}
		}
		else if (ht instanceof Function) {
		
			/*
			 * if it's a function we need to get the nested value if its a
			 * datatype function or we need to do the CONCAT if its URI(....).
			 */
			Function ov = (Function) ht;
			Predicate function = ov.getFunctionSymbol();

			/*
			 * Adding the column(s) with the actual value(s)
			 */
			if (function instanceof DatatypePredicate) {
				/*
				 * Case where we have a typing function in the head (this is the
				 * case for all literal columns
				 */
				String termStr = null;
				int size = ov.getTerms().size();
				if ((function instanceof Literal) || size > 2 )
				{
					termStr = getSQLStringForTemplateFunction(ov, index);
				}
				else {
					Term term = ov.getTerms().get(0);
					if (term instanceof ValueConstant) {
						termStr = getSQLLexicalForm((ValueConstant) term);
					} else {
						termStr = getSQLString(term, index, false);
						if(function.getName().equals("http://www.opengis.net/ont/geosparql#wktLiteral") && transformGeoToWKT){
							termStr = "ST_AsText(" + termStr + ")";
						}
					}
				}
				mainColumn = termStr;

			}
			else if (function instanceof URITemplatePredicate) {
				// New template based URI building functions
				mainColumn = getSQLStringForTemplateFunction(ov, index);
			}
			else if (function instanceof BNodePredicate) {
				// New template based BNODE building functions
				mainColumn = getSQLStringForTemplateFunction(ov, index);

			}
			else if (function instanceof StringOperationPredicate || function instanceof DateTimeOperationPredicate) {
				// Functions returning string values
				mainColumn = getSQLString(ov, index, false);
			}
			else if (function instanceof NonBooleanOperationPredicate){
			 	if (function.equals(OBDAVocabulary.UUID)) {
				 mainColumn = sqladapter.uuid();
				} else if (function.equals(OBDAVocabulary.STRUUID)) {
				 mainColumn = sqladapter.strUuid();
			 	}
			}

            else if (function.isArithmeticPredicate()){
            	String expressionFormat = getNumericalOperatorString(function);
            	if (isBinary(ov)) {
            		Term right = ov.getTerm(1);
            		Term left = ov.getTerm(0);
                    String leftOp = getSQLString(left, index, true);
            		String rightOp = getSQLString(right, index, true);
            		mainColumn = String.format("(" + expressionFormat + ")", leftOp, rightOp);
              	}
                
            	else if (isUnary(ov)) {
            		Term left = ov.getTerm(0);
                    String leftOp = getSQLString(left, index, true);
                    mainColumn = String.format("(" + expressionFormat + ")", leftOp);
            	}
            	else if (ov.getArity()==3) {

					if(ov.getFunctionSymbol().getName().equals(OBDAVocabulary.SFDISTANCE.getName())) {
						//check if arg is constant and add ST_GeomFromWKT
						if(ov.getTerms().get(0) instanceof ValueConstant){
							ov.setTerm(0, OBDADataFactoryImpl.getInstance().getFunctionGeomFromWKT(ov.getTerms().get(0)));
						}
						if(ov.getTerms().get(1) instanceof ValueConstant){
							ov.setTerm(1, OBDADataFactoryImpl.getInstance().getFunctionGeomFromWKT(ov.getTerms().get(1)));
						}
					}

            		Term term1 = ov.getTerms().get(0);
            		Term term2 = ov.getTerms().get(1);

					String rightOp = getSQLString(term2, index, true);
					String leftOp = getSQLString(term1, index, true);
					Term term3 = ov.getTerms().get(2);
					String thrirdOp = getSQLString(term3, index, true);
					if(ov.getFunctionSymbol().getName().equals(OBDAVocabulary.SFDISTANCE.getName())) {
						mainColumn = "(" + sqladapter.strEncodeForSpatialDistance(leftOp, rightOp, term3) +")";
					}
					else {
						mainColumn =String.format("(" + expressionFormat + ")", leftOp, rightOp, thrirdOp);
					}
            	}
            	else { mainColumn = expressionFormat; }
            } else {
				throw new IllegalArgumentException(
						"Error generating SQL query. Found an invalid function during translation: "
								+ ov.toString());
			}
		} else {
			throw new RuntimeException("Cannot generate SELECT for term: " + ht.toString());
		}

		/*
		 * If we have a column we need to still CAST to VARCHAR
		 */
		if (mainColumn.charAt(0) != '\'' && mainColumn.charAt(0) != '(') {
			if (!isStringColType(ht, index)) {
				if (isGeomColType(ht, index))
					mainColumn =  sqladapter.sqlCast(mainColumn, 1111);
				else
					mainColumn = sqladapter.sqlCast(mainColumn, Types.VARCHAR);
			}
		}
		return String.format(mainTemplate, mainColumn, variableName);
	}
	
	private String getLangColumnForSELECT(Term ht, List<String> signature, int hpos, QueryAliasIndex index,
										  Set<String> sqlVariableNames) {

		/**
		 * Creates a variable name that fits to the restrictions of the SQL dialect.
		 */
		String langVariableName = sqladapter.nameTopVariable(signature.get(hpos), langSuffix, sqlVariableNames);
		sqlVariableNames.add(langVariableName);

		if (ht instanceof Function) {
            Function ov = (Function) ht;
            Predicate function = ov.getFunctionSymbol();

            String lang = getLangType(ov, index);
//

            return (String.format(langStrForSELECT, lang, langVariableName));
        }

        return  (String.format(langStrForSELECT, "NULL", langVariableName));

	}

    private String getLangType(Function func1, QueryAliasIndex index) {

        Predicate pred1 = func1.getFunctionSymbol();

        if (dtfac.isLiteral(pred1) && isBinary(func1)) {


            Term langTerm = func1.getTerm(1);
            if (langTerm == OBDAVocabulary.NULL) {
                return  "NULL";
            } else if (langTerm instanceof ValueConstant) {
                return getSQLLexicalForm((ValueConstant) langTerm);
            } else {
                return getSQLString(langTerm, index, false);
            }

        }
        else if (pred1.isStringOperationPredicate()) {

            if(pred1.equals(OBDAVocabulary.CONCAT)) {
                Term concat1 = func1.getTerm(0);
                Term concat2 = func1.getTerm(1);

                if (concat1 instanceof Function && concat2 instanceof Function) {
                    Function concatFunc1 = (Function) concat1;
                    Function concatFunc2 = (Function) concat2;

                    String lang1 = getLangType(concatFunc1, index);

                    String lang2 = getLangType(concatFunc2, index);

                    if (lang1.equals(lang2)) {

                        return lang1;

                    } else return "NULL";

                }
            }
            else if(pred1.equals(OBDAVocabulary.REPLACE)){
                Term rep1 = func1.getTerm(0);


                if (rep1 instanceof Function) {
                    Function replFunc1 = (Function) rep1;


                    String lang1 = getLangType(replFunc1, index);

                    return lang1;



                }

            }
            return "NULL";
        }
        else return "NULL";
    }

	

	/**
	 * Beware: a new entry will be added to sqlVariableNames (is thus mutable).
	 */
	private String getTypeColumnForSELECT(Term ht, List<String> signature, int hpos,
										  Set<String> sqlVariableNames) {
		

		COL_TYPE type = getTypeColumn(ht);

		int code = type.getQuestCode();

            /**
             * Creates a variable name that fits to the restrictions of the SQL dialect.
             */
            String typeVariableName = sqladapter.nameTopVariable(signature.get(hpos), typeSuffix, sqlVariableNames);
            sqlVariableNames.add(typeVariableName);    

        return String.format(typeStrForSELECT, code, typeVariableName);
	}

    private COL_TYPE getTypeColumn(Term ht) {
        COL_TYPE type;

        if (ht instanceof Function) {
			Function ov = (Function) ht;
			Predicate function = ov.getFunctionSymbol();

			/*
			 * Adding the ColType column to the projection (used in the result
			 * set to know the type of constant)
			 *
			 * NOTE NULL is IDENTIFIER 0 in QuestResultSet do not USE for any
			 * type
			 */

			if (function instanceof URITemplatePredicate || function.equals(OBDAVocabulary.UUID)) {
				type = COL_TYPE.OBJECT;
			} else if (function.equals(OBDAVocabulary.GEOSPARQL_WKT_LITERAL_DATATYPE)) {
				//return (String.format(typeStr, 10, signature.get(hpos)));
				type = COL_TYPE.GEOMETRY;
			}   else if (function instanceof BNodePredicate) {
	                type = COL_TYPE.BNODE;
			} else if (function.isStringOperationPredicate() || function instanceof NonBooleanOperationPredicate) {
            
        
            //else if (function.isStringOperationPredicate()) {


				if (function.equals(OBDAVocabulary.CONCAT)) {

					COL_TYPE type1, type2;

					type1 = getTypeColumn(ov.getTerm(0));
					type2 = getTypeColumn(ov.getTerm(1));

					if (type1.equals(type2) && (type1.equals(COL_TYPE.STRING))) {

						type = type1; //only if both values are string return string

					} else {

						type = COL_TYPE.LITERAL;
					}

				} else if (function.equals(OBDAVocabulary.REPLACE)) {
					COL_TYPE type1;
					type1 = getTypeColumn(ov.getTerm(0));

					if (type1.equals(COL_TYPE.STRING)) {
						type = type1;
					} else {
						type = COL_TYPE.LITERAL;
					}

				} else if (function.equals(OBDAVocabulary.STRLEN)) {

					type = COL_TYPE.INTEGER;

				} else {

					type = COL_TYPE.LITERAL;
				}

			} else if (ov.isArithmeticFunction()) {

				if (function.equals(OBDAVocabulary.ABS) || function.equals(OBDAVocabulary.ROUND) ||
						function.equals(OBDAVocabulary.CEIL) || function.equals(OBDAVocabulary.FLOOR) ) {
					type = getTypeColumn(ov.getTerm(0));

				}else if (function.equals(OBDAVocabulary.RAND)){
					type = COL_TYPE.DOUBLE;

				} else {
					type = COL_TYPE.LITERAL;
				}
			} else if (ov.isDateTimeFunction()) {

				if (function.equals(OBDAVocabulary.MONTH) || function.equals(OBDAVocabulary.YEAR) ||
						function.equals(OBDAVocabulary.DAY) || function.equals(OBDAVocabulary.MINUTES) || function.equals(OBDAVocabulary.HOURS)) {
					type = COL_TYPE.INTEGER;

				} else if (function.equals(OBDAVocabulary.NOW)) {
					type = COL_TYPE.DATETIME;
				} else if (function.equals(OBDAVocabulary.SECONDS)) {
					type = COL_TYPE.DECIMAL;
				}
				else{
					type = COL_TYPE.LITERAL;
				}
			} else {

//                type = dtfac.getDatatype(functionString);
				type = function.getType(0);
			}
		}
        else if (ht instanceof URIConstant) {
            type = COL_TYPE.OBJECT;
		}
		else if (ht == OBDAVocabulary.NULL) {  // NULL is an instance of ValueConstant
            type = COL_TYPE.NULL;
        }
        else
            throw new RuntimeException("Cannot generate SELECT for term: " + ht.toString());


        return type;
    }


    public String getSQLStringForTemplateFunction(Function ov, QueryAliasIndex index) {
		/*
		 * The first inner term determines the form of the result
		 */
		Term t = ov.getTerms().get(0);
		Term c;
	
		String literalValue = "";
		
		if (t instanceof ValueConstant || t instanceof BNode) {
			/*
			 * The function is actually a template. The first parameter is a
			 * string of the form http://.../.../ or empty "{}" with place holders of the form
			 * {}. The rest are variables or constants that should be put in
			 * place of the palce holders. We need to tokenize and form the
			 * CONCAT
			 */
			if (t instanceof BNode) {
				c = (BNode) t;
				literalValue = ((BNode) t).getName();
			} else {
				c = (ValueConstant) t;	
				literalValue = ((ValueConstant) t).getValue();
			}		
			Predicate pred = ov.getFunctionSymbol();




			String replace1;
            String replace2;
            if(generatingREPLACE) {

                replace1 = "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(" +
                        "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(";

                replace2 = ",' ', '%20')," +
                        "'!', '%21')," +
                        "'@', '%40')," +
                        "'#', '%23')," +
                        "'$', '%24')," +
                        "'&', '%26')," +
                        "'*', '%42'), " +
                        "'(', '%28'), " +
                        "')', '%29'), " +
                        "'[', '%5B'), " +
                        "']', '%5D'), " +
                        "',', '%2C'), " +
                        "';', '%3B'), " +
                        "':', '%3A'), " +
                        "'?', '%3F'), " +
                        "'=', '%3D'), " +
                        "'+', '%2B'), " +
                        "'''', '%22'), " +
                        "'/', '%2F')";
            } else {
                replace1 = replace2 = "";
            }

            String template = trimLiteral(literalValue);
            
			String[] split = template.split("[{][}]");
			
			List<String> vex = new LinkedList<String>();
			if (split.length > 0 && !split[0].isEmpty()) {
				vex.add(sqladapter.getSQLLexicalFormString(split[0]));
			}
			
			/*
			 * New we concat the rest of the function, note that if there is only 1 element
			 * there is nothing to concatenate
			 */
			if (ov.getTerms().size() > 1) {
				int size = ov.getTerms().size();
				if (dtfac.isLiteral(pred)) {
					size--;
				}
				for (int termIndex = 1; termIndex < size; termIndex++) {
					Term currentTerm = ov.getTerms().get(termIndex);
					String repl = "";
					if (isStringColType(currentTerm, index)) {
						repl = replace1 + (getSQLString(currentTerm, index, false)) + replace2;
					} else {
						//if (isGeomColType(currentTerm, index)){
						//	repl =  sqladapter.sqlCast(getSQLString(currentTerm, index, false), 1111);
						//}
						//else
						repl = replace1 + sqladapter.sqlCast(getSQLString(currentTerm, index, false), Types.VARCHAR) + replace2;
					}
					vex.add(repl);
					if (termIndex < split.length ) {
						vex.add(sqladapter.getSQLLexicalFormString(split[termIndex]));
					}
				}
			}
		
			if (vex.size() == 1) {
				
				return vex.get(0);
			}
			String[] params = new String[vex.size()];
			int i = 0;
			for (String param : vex) {
				params[i] = param;
				i += 1;
			}
			return getStringConcatenation(sqladapter, params);
			
		} else if (t instanceof Variable) {
			/*
			 * The function is of the form uri(x), we need to simply return the
			 * value of X
			 */
			return sqladapter.sqlCast(getSQLString(t, index, false), Types.VARCHAR);
			
		} else if (t instanceof URIConstant) {
			/*
			 * The function is of the form uri("http://some.uri/"), i.e., a
			 * concrete URI, we return the string representing that URI.
			 */
			URIConstant uc = (URIConstant) t;
			return sqladapter.getSQLLexicalFormString(uc.getIRI());
		}

		/*
		 * Unsupported case
		 */
		throw new IllegalArgumentException("Error, cannot generate URI constructor clause for a term: " + ov.toString());

	}

	// TODO: move to SQLAdapter
	private String getStringConcatenation(SQLDialectAdapter adapter, String[] params) {
		return sqladapter.strConcat(params);
		
	}

	private boolean isGeomColType(Term term, QueryAliasIndex index) {
		if (term instanceof Function) {
			Function function = (Function) term;
			Predicate functionSymbol = function.getFunctionSymbol();
			if (isUnary(function)) {
					/*
					 * Update the term with the parent term's first parameter.
					 * Note: this method is confusing :(
					 */
					 term = function.getTerm(0);
					 return isGeomColType(term, index);
				}
			
		} else if (term instanceof Variable) {
			Set<QualifiedAttributeID> viewdef = index.getColumnReferences((Variable) term);
			QualifiedAttributeID def = viewdef.iterator().next();
			QuotedID attributeId = def.getAttribute();
			RelationID tableId = null;
			if (def.getRelation().getTableName().toUpperCase().startsWith(VIEW_NAME_PREFIX)) {
				for (Map.Entry<Function, RelationID> entry : index.viewNames.entrySet()) {
					RelationID value = entry.getValue();
					if (value.equals(def.getRelation())) {
						tableId = Relation2DatalogPredicate
									.createRelationFromPredicateName(entry.getKey().getFunctionSymbol());
						break;
					}
				}
			}
			DatabaseRelationDefinition table = metadata.getDatabaseRelation(tableId);
			if (table != null) {
	 			Attribute a = table.getAttribute(attributeId);
				if (a.getSQLTypeName().equals("geometry"))
					return true;
				else
					return false;
			}
		}
		return false;

	}


	
	private boolean isStringColType(Term term, QueryAliasIndex index) {
		if (term instanceof Function) {
			Function function = (Function) term;
			Predicate functionSymbol = function.getFunctionSymbol();

			if (functionSymbol instanceof URITemplatePredicate) {
				/*
				 * A URI function always returns a string, thus it is a string column type.
				 */
				return true;
			} 
			else {
				if (isUnary(function)) {

					/*
					 * Update the term with the parent term's first parameter.
					 * Note: this method is confusing :(
					 */
					term = function.getTerm(0);
					return isStringColType(term, index);
				}
			}
		} 
		else if (term instanceof Variable) {
			Set<QualifiedAttributeID> viewdef = index.getColumnReferences((Variable) term);
			QualifiedAttributeID def = viewdef.iterator().next();
			QuotedID attributeId = def.getAttribute();
			RelationID tableId = null;
			// ROMAN (8 Oct 2015)
			// case conversion to be removed
			if (def.getRelation().getTableName().toUpperCase().startsWith(VIEW_NAME_PREFIX)) {
				for (Map.Entry<Function, RelationID> entry : index.viewNames.entrySet()) {
					RelationID value = entry.getValue();
					if (value.equals(def.getRelation())) {
						tableId = Relation2DatalogPredicate
									.createRelationFromPredicateName(entry.getKey().getFunctionSymbol());
						break;
					}
				}
			}
			DatabaseRelationDefinition table = metadata.getDatabaseRelation(tableId);
			if (table != null) {
				// ROMAN (15 Oct 2015): i'm not sure what to do if it is a view (i.e., a complex subquery)
	 			Attribute a = table.getAttribute(attributeId);
				switch (a.getType()) {
					case Types.VARCHAR:
					// case Types.CHAR: // ROMAN (10 Oct 2015) -- otherwise PgsqlDatatypeTest.all fails 
					case Types.LONGNVARCHAR:
					case Types.LONGVARCHAR:
					case Types.NVARCHAR:
					case Types.NCHAR:
						return true;
					default:
						return false;
				}
			}
		}
		return false;
	}

	private static final Pattern pQuotes = Pattern.compile("[\"`\\['][^\\.]*[\"`\\]']");
	
	private static String trimLiteral(String string) {
		while (pQuotes.matcher(string).matches()) {
			string = string.substring(1, string.length() - 1);
		}
		return string;
	}

	/**
	 * Determines if it is a unary function.
	 */
	private boolean isUnary(Function fun) {
		return (fun.getArity() == 1);
	}

	/**
	 * Determines if it is a binary function.
	 */
	private boolean isBinary(Function fun) {
		return (fun.getArity() == 2);
	}

	/**
	 * Generates the SQL string that forms or retrieves the given term. The
	 * function takes as input either: a constant (value or URI), a variable, or
	 * a Function (i.e., uri(), eq(..), ISNULL(..), etc)).
	 * <p>
	 * If the input is a constant, it will return the SQL that generates the
	 * string representing that constant.
	 * <p>
	 * If its a variable, it returns the column references to the position where
	 * the variable first appears.
	 * <p>
	 * If its a function uri(..) it returns the SQL string concatenation that
	 * builds the result of uri(...)
	 * <p>
	 * If its a boolean comparison, it returns the corresponding SQL comparison.
	 */
	public String getSQLString(Term term, QueryAliasIndex index, boolean useBrackets) {
		if (term == null) {
			return "";
		}
		if (term instanceof ValueConstant) {
			ValueConstant ct = (ValueConstant) term;

			return getSQLLexicalForm(ct);
		} 
		else if (term instanceof URIConstant) {

			URIConstant uc = (URIConstant) term;
			return sqladapter.getSQLLexicalFormString(uc.toString());
		} 
		else if (term instanceof Variable) {
			Variable var = (Variable) term;
			Set<QualifiedAttributeID> posList = index.getColumnReferences(var);
			if (posList == null || posList.size() == 0) {
				throw new RuntimeException("Unbound variable found in WHERE clause: " + term);
			}
			return posList.iterator().next().getSQLRendering();
		}

		/* If its not constant, or variable its a function */

		Function function = (Function) term;
		List<Term> terms = function.getTerms();
		Term term1 = null;
		if (terms.size() > 0){
			 term1 = function.getTerms().get(0);
		} 

		Predicate functionSymbol = function.getFunctionSymbol();
		int size = function.getTerms().size();

		if (functionSymbol instanceof DatatypePredicate) {
			if (functionSymbol.getType(0) == COL_TYPE.UNSUPPORTED) {
				throw new RuntimeException("Unsupported type in the query: " + function);
			}
			if (size == 1) {
				// atoms of the form integer(x)
				return getSQLString(term1, index, false);
			} else 	{
				return getSQLStringForTemplateFunction(function, index);
			}
		} else if (functionSymbol.isBooleanPredicate() ) {
			// atoms of the form EQ(x,y)
			String expressionFormat = getBooleanOperatorString(functionSymbol);
			if (isUnary(function)) {
				// for unary functions, e.g., NOT, IS NULL, IS NOT NULL
				// also added for IS TRUE
				if (expressionFormat.contains("IS TRUE")) {
					// find data type of term and evaluate accordingly
					String column = getSQLString(term1, index, false);
					int type = getVariableDataType(term1);
					if (type == Types.INTEGER) return String.format("%s > 0", column);
                    if (type == Types.BIGINT) return String.format("%s > 0", column);
					if (type == Types.DOUBLE) return String.format("%s > 0", column);
                    if (type == Types.FLOAT) return String.format("%s > 0", column);
					if (type == Types.BOOLEAN) return String.format("%s", column);
					if (type == Types.VARCHAR) return String.format("LENGTH(%s) > 0", column);
					return "1";
				}
				String op = getSQLString(term1, index, true);
				return String.format(expressionFormat, op);
				
			} else if (isBinary(function)) {
				// for binary functions, e.g., AND, OR, EQ, NEQ, GT etc.
				String leftOp = getSQLString(term1, index, true);
				Term term2 = function.getTerms().get(1);
				String rightOp = getSQLString(term2, index, true);
				String result = String.format(expressionFormat, leftOp, rightOp);
				if (useBrackets) {
					return String.format("(%s)", result);
				} else {
					return result;
				}
			} else {
				if (functionSymbol == OBDAVocabulary.SPARQL_REGEX) {
					boolean caseinSensitive = false;
					boolean multiLine = false;
					boolean dotAllMode = false;
					if (function.getArity() == 3) {
						if (function.getTerm(2).toString().contains("i")) {
							caseinSensitive = true;
						}
						if (function.getTerm(2).toString().contains("m")) {
							multiLine = true;
						}
						if (function.getTerm(2).toString().contains("s")) {
							dotAllMode = true;
						}
					}
					Term p1 = function.getTerm(0);
					Term p2 = function.getTerm(1);
					
					String column = getSQLString(p1, index, false);
					String pattern = getSQLString(p2, index, false);
					return sqladapter.sqlRegex(column, pattern, caseinSensitive, multiLine, dotAllMode);
				}
				else
					throw new RuntimeException("Cannot translate boolean function: " + functionSymbol);
			}
			
		} else if (functionSymbol instanceof NumericalOperationPredicate) {
			String expressionFormat = getNumericalOperatorString(functionSymbol);
			String leftOp = getSQLString(term1, index, true);
				if (isUnary(function)){
					String result = String.format(expressionFormat, leftOp);
					return result;
				}
				else if (isBinary(function)) {
					Term term2 = function.getTerms().get(1);
					String rightOp = getSQLString(term2, index, true);
					String result = String.format(expressionFormat, leftOp, rightOp); 
						if (useBrackets) {
							return String.format("(%s)", result);
						} else {
							return result;
					}
				} else if (function.getArity()==3) {
					Term term2 = function.getTerms().get(1);
					String rightOp = getSQLString(term2, index, true);
					Term term3 = function.getTerms().get(2);
					String thrirdOp = getSQLString(term3, index, true);
					String result = null;
					if(function.getFunctionSymbol().getName().equals(OBDAVocabulary.SFDISTANCE.getName())) {
						result = sqladapter.strEncodeForSpatialDistance(leftOp, rightOp, term3);
					}
					else {
						result = String.format(expressionFormat, leftOp, rightOp, thrirdOp);
					}
					  
						if (useBrackets) {
							return String.format("(%s)", result);
						} else {
							return result;
					}
				}
				else {
					return expressionFormat;
				}

				
		} else {
			String functionName = functionSymbol.toString();
			if (functionName.equals(OBDAVocabulary.QUEST_CAST.getName())) {
				String columnName = getSQLString(function.getTerm(0), index, false);
				String datatype = ((Constant) function.getTerm(1)).getValue();
				int sqlDatatype = -1;
				if (datatype.equals(OBDAVocabulary.RDFS_LITERAL_URI)) {
					sqlDatatype = Types.VARCHAR;
				}
				if (isStringColType(function, index)) {
					return columnName;
				} else {
					return sqladapter.sqlCast(columnName, sqlDatatype);
				}
			} else if (functionName.equals(OBDAVocabulary.SPARQL_STR.getName())) {
				String columnName = getSQLString(function.getTerm(0), index, false);
				if (isStringColType(function, index)) {
					return columnName;
				} else {
					return sqladapter.sqlCast(columnName, Types.VARCHAR);
				}
			} else if (functionName.equals(OBDAVocabulary.REPLACE.getName())) {
				String orig = getSQLString(function.getTerm(0), index, false);
				String out_str = getSQLString(function.getTerm(1), index, false);
				String in_str = getSQLString(function.getTerm(2), index, false);
				String result = sqladapter.strReplace(orig, out_str, in_str);
				return result;
			} 
			 else if (functionName.equals(OBDAVocabulary.CONCAT.getName())) {
					String left = getSQLString(function.getTerm(0), index, false);
					String right = getSQLString(function.getTerm(1), index, false);
					String result = sqladapter.strConcat(new String[]{left, right});
					return result;
			}

			else if (functionName.equals(OBDAVocabulary.STRLEN.getName())) {
				String literal = getSQLString(function.getTerm(0), index, false);
				String result = sqladapter.strLength(literal);
				return result;
			} else if (functionName.equals(OBDAVocabulary.YEAR.getName())) {
				String literal = getSQLString(function.getTerm(0), index, false);
				String result = sqladapter.dateYear(literal);
				return result;

			} else if (functionName.equals(OBDAVocabulary.MINUTES.getName())) {
				String literal = getSQLString(function.getTerm(0), index, false);
				String result = sqladapter.dateMinutes(literal);
				return result;

			} else if (functionName.equals(OBDAVocabulary.DAY.getName())) {
				String literal = getSQLString(function.getTerm(0), index, false);
				String result = sqladapter.dateDay(literal);
				return result;

			} else if (functionName.equals(OBDAVocabulary.NOW.getName())) {
				return sqladapter.dateNow();

			}  else if (functionName.equals(OBDAVocabulary.MONTH.getName())) {
				String literal = getSQLString(function.getTerm(0), index, false);
				String result = sqladapter.dateMonth(literal);
				return result;

			}  else if (functionName.equals(OBDAVocabulary.SECONDS.getName())) {
				String literal = getSQLString(function.getTerm(0), index, false);
				String result = sqladapter.dateSeconds(literal);
				return result;

			} else if (functionName.equals(OBDAVocabulary.HOURS.getName())) {
				String literal = getSQLString(function.getTerm(0), index, false);
				String result = sqladapter.dateHours(literal);
				return result;

			} else if (functionName.equals(OBDAVocabulary.TZ.getName())) {
				String literal = getSQLString(function.getTerm(0), index, false);
				String result = sqladapter.dateTZ(literal);
				return result;

			} else if (functionName.equals(OBDAVocabulary.ENCODE_FOR_URI.getName())) {
				String literal = getSQLString(function.getTerm(0), index, false);
				String result = sqladapter.strEncodeForUri(literal);
				return result;
			}

			else if (functionName.equals(OBDAVocabulary.UCASE.getName())) {
				String literal = getSQLString(function.getTerm(0), index, false);
				String result = sqladapter.strUcase(literal);
				return result;
			}

			else if (functionSymbol.equals(OBDAVocabulary.MD5)) {
				String literal = getSQLString(function.getTerm(0), index, false);
				String result = sqladapter.MD5(literal);
				return result;

			} else if (functionSymbol.equals(OBDAVocabulary.SHA1)) {
				String literal = getSQLString(function.getTerm(0), index, false);
				String result = sqladapter.SHA1(literal);
				return result;
			} else if (functionSymbol.equals(OBDAVocabulary.SHA256)) {
				String literal = getSQLString(function.getTerm(0), index, false);
				String result = sqladapter.SHA256(literal);
				return result;
			} else if (functionSymbol.equals(OBDAVocabulary.SHA512)) {
				String literal = getSQLString(function.getTerm(0), index, false);
				String result = sqladapter.SHA512(literal); //TODO FIX
				return result;
			}

			else if (functionName.equals(OBDAVocabulary.LCASE.getName())) {
				String literal = getSQLString(function.getTerm(0), index, false);
				String result = sqladapter.strLcase(literal);
				return result;
			}  else if (functionName.equals(OBDAVocabulary.SUBSTR.getName())) {
				String string = getSQLString(function.getTerm(0), index, false);
				String start = getSQLString(function.getTerm(1), index, false);
				if(function.getTerms().size()==2){
					return sqladapter.strSubstr(string, start);
				}

				String end = getSQLString(function.getTerm(2), index, false);
				String result = sqladapter.strSubstr(string, start, end);

				return result;
			}

			else if (functionName.equals(OBDAVocabulary.STRBEFORE.getName())) {
				String string = getSQLString(function.getTerm(0), index, false);
				String before = getSQLString(function.getTerm(1), index, false);
				String result = sqladapter.strBefore(string, before);
				return result;
			}

			else if (functionName.equals(OBDAVocabulary.STRAFTER.getName())) {
				String string = getSQLString(function.getTerm(0), index, false);
				String after = getSQLString(function.getTerm(1), index, false);
				String result = sqladapter.strAfter(string, after);
				return result;
			}


		}

		/*
		 * The atom must be of the form uri("...", x, y)
		 */
		// String functionName = function.getFunctionSymbol().toString();
		if ((functionSymbol instanceof URITemplatePredicate) || (functionSymbol instanceof BNodePredicate)) {
			return getSQLStringForTemplateFunction(function, index);
		} 
		else {
			throw new RuntimeException("Unexpected function in the query: " + function);
		}
	}

	/***
	 * Returns the valid SQL lexical form of rdf literals based on the current
	 * database and the datatype specified in the function predicate.
	 * 
	 * <p>
	 * For example, if the function is xsd:boolean, and the current database is
	 * H2, the SQL lexical form would be for "true" "TRUE" (or any combination
	 * of lower and upper case) or "1" is always
	 * 
	 * @param constant
	 * @return
	 */
	private String getSQLLexicalForm(ValueConstant constant) {
		String sql = null;
		if (constant.getType() == COL_TYPE.BNODE || constant.getType() == COL_TYPE.LITERAL || constant.getType() == COL_TYPE.OBJECT
				|| constant.getType() == COL_TYPE.STRING) {
			sql = "'" + constant.getValue() + "'";
		} 
		else if (constant.getType() == COL_TYPE.BOOLEAN) {
			String value = constant.getValue();
			boolean v = parseXsdBoolean(value);
			sql = sqladapter.getSQLLexicalFormBoolean(v);
		} 
		else if (constant.getType() == COL_TYPE.DATETIME ) {
			sql = sqladapter.getSQLLexicalFormDatetime(constant.getValue());
		}
		else if (constant.getType() == COL_TYPE.DATETIME_STAMP ) {
			sql = sqladapter.getSQLLexicalFormDatetimeStamp(constant.getValue());
		}
		else if (constant.getType() == COL_TYPE.DECIMAL || constant.getType() == COL_TYPE.DOUBLE
				|| constant.getType() == COL_TYPE.INTEGER || constant.getType() == COL_TYPE.LONG
                || constant.getType() == COL_TYPE.FLOAT || constant.getType() == COL_TYPE.NON_POSITIVE_INTEGER
                || constant.getType() == COL_TYPE.INT || constant.getType() == COL_TYPE.UNSIGNED_INT
                || constant.getType() == COL_TYPE.NEGATIVE_INTEGER
                || constant.getType() == COL_TYPE.POSITIVE_INTEGER || constant.getType() == COL_TYPE.NON_NEGATIVE_INTEGER) {
			sql = constant.getValue();
		} 
		else {
			sql = "'" + constant.getValue() + "'";
		}
		return sql;

	}

	/**
	 * @see http://www.w3.org/TR/xmlschema11-2/#boolean
	 * @param value from the lexical space of xsd:boolean
	 * @return boolean
	 */

	public static boolean parseXsdBoolean(String value) {

		if (value.equals("true") || value.equals("1"))
			return true;
		else if (value.equals("false") || value.equals("0"))
			return false;

		throw new RuntimeException("Invalid lexical form for xsd:boolean. Found: " + value);
	}



	/**
	 * Returns the SQL string for the boolean operator, including placeholders
	 * for the terms to be used, e.g., %s = %s, %s IS NULL, etc.
	 * 
	 * @param functionSymbol
	 * @return
	 */
	private String getBooleanOperatorString(Predicate functionSymbol) {
		String operator = null;
		if (functionSymbol.equals(OBDAVocabulary.EQ)) {
			operator = EQ_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.NEQ)) {
			operator = NEQ_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.GT)) {
			operator = GT_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.GTE)) {
			operator = GTE_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.LT)) {
			operator = LT_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.LTE)) {
			operator = LTE_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.AND)) {
			operator = AND_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.OR)) {
			operator = OR_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.NOT)) {
			operator = NOT_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.IS_NULL)) {
			operator = IS_NULL_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.IS_NOT_NULL)) {
			operator = IS_NOT_NULL_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.IS_TRUE)) {
			operator = IS_TRUE_OPERATOR;
		} else if(functionSymbol.equals(OBDAVocabulary.OVERLAPS)){
			operator = OVERLAPS_OPERATOR;
		} else if(functionSymbol.equals(OBDAVocabulary.SFEQUALS)){
			operator = SFEQUALS_OPERATOR; 
		} else if(functionSymbol.equals(OBDAVocabulary.SFDISTANCE)){
			operator = SFDISTANCE_OPERATOR;
		}else if(functionSymbol.equals(OBDAVocabulary.SFDISJOINT)){
			operator = SFDISJOINT_OPERATOR;
		} else if(functionSymbol.equals(OBDAVocabulary.SFINTERSECTS)){
			operator = SFINTERSECTS_OPERATOR;
		} else if(functionSymbol.equals(OBDAVocabulary.SFTOUCHES)){
			operator = SFTOUCHES_OPERATOR;
		} else if(functionSymbol.equals(OBDAVocabulary.SFWITHIN)){
			operator = SFWITHIN_OPERATOR;
		} else if(functionSymbol.equals(OBDAVocabulary.SFCONTAINS)){
			operator = SFCONTAINS_OPERATOR;
		} else if(functionSymbol.equals(OBDAVocabulary.SFCROSSES)){
			operator = SFCROSSES_OPERATOR;
		} else if(functionSymbol.equals(OBDAVocabulary.EHEQUALS)){
			operator = EHEQUALS_OPERATOR;
		} else if(functionSymbol.equals(OBDAVocabulary.EHDISJOINT)){
			operator = EHDISJOINT_OPERATOR;
		} else if(functionSymbol.equals(OBDAVocabulary.EHOVERLAPS)){
			operator = EHOVERLAP_OPERATOR;
		}else if(functionSymbol.equals(OBDAVocabulary.EHCOVERS)){
			operator = EHCOVERS_OPERATOR;
		} else if(functionSymbol.equals(OBDAVocabulary.EHCOVEREDBY)){
			operator = EHCOVEREDBY__OPERATOR;
		} else if(functionSymbol.equals(OBDAVocabulary.EHINSIDE)){
			operator = EHINSIDE_OPERATOR;
		} else if(functionSymbol.equals(OBDAVocabulary.EHCONTAINS)){
			operator = EHCONTAINS_OPERATOR;
		}  else if(functionSymbol.equals(OBDAVocabulary.GEOMFROMWKT)){
				operator = GEOMFROMWKT_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.SPARQL_LIKE)) {
			operator = LIKE_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.SPARQL_REGEX)) {
			operator = ""; //we do not need the operator for regex, it should not be used, because the sql adapter will take care of this
		} else if (functionSymbol.equals(OBDAVocabulary.STR_STARTS)) {
			operator = sqladapter.strStartsOperator();
		} else if (functionSymbol.equals(OBDAVocabulary.STR_ENDS)) {
			operator = sqladapter.strEndsOperator();
		} else if (functionSymbol.equals(OBDAVocabulary.CONTAINS)) {
				operator = sqladapter.strContainsOperator();
		}
		else if (functionSymbol.equals(OBDAVocabulary.SPARQL_IN)) {
			operator = IN_OPERATOR;
		}
		else {
			throw new RuntimeException("Unknown boolean operator: " + functionSymbol);
		}
		return operator;
	}

	private String getNumericalOperatorString(Predicate functionSymbol) {
		String operator;
		if (functionSymbol.equals(OBDAVocabulary.ADD)) {
			operator = ADD_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.SUBTRACT)) {
			operator = SUBTRACT_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.MULTIPLY)) {
			operator = MULTIPLY_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.ABS)) {
				operator = ABS_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.CEIL)) {
			operator = sqladapter.ceil();
		} else if (functionSymbol.equals(OBDAVocabulary.FLOOR)) {
			operator = FLOOR_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.ROUND)) {
			operator = sqladapter.round();
		} else if (functionSymbol.equals(OBDAVocabulary.RAND)) {
			operator = sqladapter.rand();
		} 
		else if (functionSymbol.equals(OBDAVocabulary.SFDISTANCE)) {
			operator = SFDISTANCE_OPERATOR;
		}else {
			throw new RuntimeException("Unknown numerical operator: " + functionSymbol);
		}
		return operator;
	}
    /**
	 * Utility class to resolve "database" atoms to view definitions ready to be
	 * used in a FROM clause, and variables, to column references defined over
	 * the existing view definitions of a query.
	 */
	public class QueryAliasIndex {

		final Map<Function, RelationID> viewNames = new HashMap<>();
		final Map<Function, RelationDefinition> dataDefinitions = new HashMap<>();
		final Map<Variable, Set<QualifiedAttributeID>> columnReferences = new HashMap<>();
		
		int dataTableCount = 0;
		boolean isEmpty = false;

		public QueryAliasIndex(CQIE query) {
			List<Function> body = query.getBody();
			generateViews(body);
		}

		private void generateViews(List<Function> atoms) {
			for (Function atom : atoms) {
				/*
				 * This will be called recursively if necessary
				 */
				generateViewsIndexVariables(atom);
			}
		}

		/***
		 * We assiciate each atom to a view definition. This will be
		 * <p>
		 * "tablename" as "viewX" or
		 * <p>
		 * (some nested sql view) as "viewX"
		 * 
		 * <p>
		 * View definitions are only done for data atoms. Join/LeftJoin and
		 * boolean atoms are not associated to view definitions.
		 * 
		 * @param atom
		 */
		private void generateViewsIndexVariables(Function atom) {
			if (atom.getFunctionSymbol() instanceof BooleanOperationPredicate) {
				return;
			} 
			else if (atom.getFunctionSymbol() instanceof AlgebraOperatorPredicate) {
				List<Term> lit = atom.getTerms();
				for (Term subatom : lit) {
					if (subatom instanceof Function) {
						generateViewsIndexVariables((Function) subatom);
					}
				}
			}

			RelationID id = Relation2DatalogPredicate.createRelationFromPredicateName(atom.getFunctionSymbol());
			RelationDefinition def = metadata.getRelation(id);
			if (def == null) {
				// There is no definition for this atom, its not a database
				// predicate, the query is empty.
				isEmpty = true;
				return;
			}
			dataTableCount++;
			viewNames.put(atom, metadata.getQuotedIDFactory().createRelationID(null, String.format(VIEW_NAME, dataTableCount)));
			dataDefinitions.put(atom, def);
			
			indexVariables(atom);
		}

		private void indexVariables(Function atom) {
			RelationDefinition def = dataDefinitions.get(atom);
			RelationID viewName = viewNames.get(atom);
			for (int index = 0; index < atom.getTerms().size(); index++) {
				Term term = atom.getTerms().get(index);
				if (!(term instanceof Variable)) {
					continue;
				}
				Set<QualifiedAttributeID> references = columnReferences.get(term);
				if (references == null) {
					references = new LinkedHashSet<>();
					columnReferences.put((Variable) term, references);
				}
				QuotedID columnId = def.getAttribute(index + 1).getID();   // indexes from 1
				QualifiedAttributeID qualifiedId = new QualifiedAttributeID(viewName, columnId);
				references.add(qualifiedId);
			}
		}

		/***
		 * Returns all the column aliases that correspond to this variable,
		 * across all the DATA atoms in the query (not algebra operators or
		 * boolean conditions.
		 * 
		 * @param var
		 *            The variable we want the referenced columns.
		 */
		public Set<QualifiedAttributeID> getColumnReferences(Variable var) {
			return columnReferences.get(var);
		}

		/***
		 * Generates the view definition, i.e., "tablename viewname".
		 */
		public String getViewDefinition(Function atom) {
			if(atom.getFunctionSymbol().toString().startsWith(OBDAVocabulary.TEMP_VIEW_QUERY)) {
				return schemaPrefix + atom.getFunctionSymbol().toString() +" "+viewNames.get(atom).getSQLRendering();
			}
			RelationDefinition def = dataDefinitions.get(atom);
			if (def instanceof DatabaseRelationDefinition) {
				return sqladapter.sqlTableName(dataDefinitions.get(atom).getID().getSQLRendering(), 
									viewNames.get(atom).getSQLRendering());
			} 
			else if (def instanceof ParserViewDefinition) {
				return String.format("(%s) %s", ((ParserViewDefinition) def).getStatement(), 
								viewNames.get(atom).getSQLRendering());
			}
			throw new RuntimeException("Impossible to get data definition for: " + atom + ", type: " + def);
		}

		public String getColumnReference(Function atom, int column) {
			RelationID viewName = viewNames.get(atom);
			RelationDefinition def = dataDefinitions.get(atom);
			QuotedID columnname = def.getAttribute(column + 1).getID(); // indexes from 1
			return new QualifiedAttributeID(viewName, columnname).getSQLRendering();
		}
	}
}
