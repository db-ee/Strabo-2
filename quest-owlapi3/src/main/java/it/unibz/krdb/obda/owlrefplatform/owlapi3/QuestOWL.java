package it.unibz.krdb.obda.owlrefplatform.owlapi3;

/*
 * #%L
 * ontop-quest-owlapi3
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

import it.unibz.krdb.obda.model.OBDAException;
import it.unibz.krdb.obda.model.OBDAModel;
import it.unibz.krdb.obda.model.ResultSet;
import it.unibz.krdb.obda.model.TupleResultSet;
import it.unibz.krdb.obda.ontology.*;
import it.unibz.krdb.obda.owlapi3.OWLAPI3TranslatorUtility;
import it.unibz.krdb.obda.owlrefplatform.core.*;
import it.unibz.krdb.obda.owlrefplatform.core.mappingprocessing.TMappingExclusionConfig;
import it.unibz.krdb.obda.utils.VersionInfo;
import it.unibz.krdb.sql.ImplicitDBConstraintsReader;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.reasoner.InconsistentOntologyException;
import org.semanticweb.owlapi.reasoner.*;
import org.semanticweb.owlapi.reasoner.impl.*;
import org.semanticweb.owlapi.reasoner.structural.StructuralReasoner;
import org.semanticweb.owlapi.util.OWLObjectPropertyManager;
import org.semanticweb.owlapi.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;

/**
 * The OBDAOWLReformulationPlatform implements the OWL reasoner interface and is
 * the implementation of the reasoning method in the reformulation project.
 */
public class QuestOWL extends OWLReasonerBase implements AutoCloseable {

	// //////////////////////////////////////////////////////////////////////////////////////
	//
	// From Structural Reasoner (to be removed later)
	//
	// //////////////////////////////////////////////////////////////////////////////////////

	//private ClassHierarchyInfo classHierarchyInfo = new ClassHierarchyInfo();

	//private ObjectPropertyHierarchyInfo objectPropertyHierarchyInfo = new ObjectPropertyHierarchyInfo();

	//private DataPropertyHierarchyInfo dataPropertyHierarchyInfo = new DataPropertyHierarchyInfo();

	private Version version;

	private boolean interrupted = false;

	private ReasonerProgressMonitor pm;

	private boolean prepared = false;

	private boolean questready = false;

	private Object inconsistent = null;

	// / holds the error that quest had when initializing
	private String errorMessage = "";

	private Exception questException = null;

	// //////////////////////////////////////////////////////////////////////////////////////
	//
	// From Quest
	//
	// //////////////////////////////////////////////////////////////////////////////////////

	/* The merge and tranlsation of all loaded ontologies */
	private Ontology translatedOntologyMerge;

	private OBDAModel obdaModel = null;

	private QuestPreferences preferences = new QuestPreferences();

	private Quest questInstance = null;

	private static Logger log = LoggerFactory.getLogger(QuestOWL.class);

	private QuestConnection conn = null;

	private QuestOWLConnection owlconn = null;

	private OWLOntologyManager man;


	// //////////////////////////////////////////////////////////////////////////////////////
	//
	//  User Constraints are primary and foreign keys not in the database
	//
	//
	// //////////////////////////////////////////////////////////////////////////////////////

	private ImplicitDBConstraintsReader userConstraints = null;

	/* Used to signal whether to apply the user constraints above */
	private boolean applyUserConstraints = false;

	// //////////////////////////////////////////////////////////////////////////////////////
	//  Davide>
	//  T-Mappings Configuration
	//
	//
	// //////////////////////////////////////////////////////////////////////////////////////

	private TMappingExclusionConfig excludeFromTMappings = TMappingExclusionConfig.empty();
	StructuralReasoner structuralReasoner;

	/* Used to signal whether to apply the user constraints above */
	//private boolean applyExcludeFromTMappings = false;

	/**
	 * Initialization code which is called from both of the two constructors.
	 * @param obdaModel
	 *
	 */
	private void init(OWLOntology rootOntology, OBDAModel obdaModel, OWLReasonerConfiguration configuration, Properties preferences){
		pm = configuration.getProgressMonitor();
		if (pm == null) {
			pm = new NullReasonerProgressMonitor();
		}

		man = rootOntology.getOWLOntologyManager();

		if (obdaModel != null)
			this.obdaModel = (OBDAModel)obdaModel.clone();

		this.preferences.putAll(preferences);
		this.structuralReasoner = new StructuralReasoner(rootOntology, configuration, BufferingMode.BUFFERING);

		extractVersion();

		prepareReasoner();
	}

	/***
	 * Default constructor.
	 */
	public QuestOWL(OWLOntology rootOntology, OBDAModel obdaModel, OWLReasonerConfiguration configuration, BufferingMode bufferingMode,
			Properties preferences) {
		super(rootOntology, configuration, bufferingMode);
		this.init(rootOntology, obdaModel, configuration, preferences);

	}

	/**
	 * This constructor is the same as the default constructor, except that extra constraints (i.e. primary and foreign keys) may be
	 * supplied
	 * @param userConstraints User-supplied primary and foreign keys
	 */
	public QuestOWL(OWLOntology rootOntology, OBDAModel obdaModel, OWLReasonerConfiguration configuration, BufferingMode bufferingMode,
			Properties preferences, ImplicitDBConstraintsReader userConstraints) {
		super(rootOntology, configuration, bufferingMode);

		this.userConstraints = userConstraints;
		assert(userConstraints != null);
		this.applyUserConstraints = true;

		this.init(rootOntology, obdaModel, configuration, preferences);
	}

	/**
	 * This constructor is the same as the default constructor,
	 * plus the list of predicates for which TMappings reasoning
	 * should be disallowed is supplied
	 * @param excludeFromTMappings from TMappings User-supplied predicates for which TMappings should be forbidden
	 */
	public QuestOWL(OWLOntology rootOntology, OBDAModel obdaModel, OWLReasonerConfiguration configuration, BufferingMode bufferingMode,
			Properties preferences, TMappingExclusionConfig excludeFromTMappings) {
		super(rootOntology, configuration, bufferingMode);

		// Davide> T-Mappings handling
		this.excludeFromTMappings = excludeFromTMappings;
		assert(excludeFromTMappings != null);

		this.init(rootOntology, obdaModel, configuration, preferences);

	}

	/**
	 * This constructor is the same as the default constructor plus the extra constraints,
	 * but the list of predicates for which TMappings reasoning should be disallowed is
	 * supplied
	 * @param excludeFromTMappings User-supplied predicates for which TMappings should be forbidden
	 */
	public QuestOWL(OWLOntology rootOntology, OBDAModel obdaModel, OWLReasonerConfiguration configuration, BufferingMode bufferingMode,
			Properties preferences, ImplicitDBConstraintsReader userConstraints,
			TMappingExclusionConfig excludeFromTMappings) {
		super(rootOntology, configuration, bufferingMode);

		this.userConstraints = userConstraints;
		assert(userConstraints != null);
		this.applyUserConstraints = true;

		this.excludeFromTMappings = excludeFromTMappings;
		assert(excludeFromTMappings != null);
		//this.applyExcludeFromTMappings = true;

		this.init(rootOntology, obdaModel, configuration, preferences);
	}
	/**
	 * extract version from {@link it.unibz.krdb.obda.utils.VersionInfo}, which is from the file {@code version.properties}
	 */
	private void extractVersion() {
		VersionInfo versionInfo = VersionInfo.getVersionInfo();
		String versionString = versionInfo.getVersion();
		String[] splits = versionString.split("\\.");
		int major = 0;
		int minor = 0;
		int patch = 0;
		int build = 0;
		try {
			major = Integer.parseInt(splits[0]);
			minor = Integer.parseInt(splits[1]);
			patch = Integer.parseInt(splits[2]);
			build = Integer.parseInt(splits[3]);
		} catch (Exception ignored) {

		}
		version = new Version(major, minor, patch, build);
	}

	public void flush() {
		prepared = false;

		super.flush();

		prepareReasoner();

	}

	@Deprecated // used in one test only
	public Quest getQuestInstance() {
		return questInstance;
	}

	public void setPreferences(QuestPreferences preferences) {
		this.preferences = preferences;
	}

	public QuestOWLStatement getStatement(NodeSelectivityEstimator nse) throws OWLException {
		if (!questready) {
			OWLReasonerRuntimeException owlReasonerRuntimeException = new ReasonerInternalException(
					"Quest was not initialized properly. This is generally indicates, connection problems or error during ontology or mapping pre-processing. \n\nOriginal error message:\n" + questException.getMessage()) ;
				owlReasonerRuntimeException.setStackTrace(questException.getStackTrace());
			throw owlReasonerRuntimeException;
		}
		return owlconn.createStrabonStatement(nse);
	}

	private void prepareQuestInstance() throws Exception {

		try {
			if (questInstance != null)
				questInstance.dispose();
		} catch (Exception e) {
			log.debug(e.getMessage());
		}

		log.debug("Initializing a new Quest instance...");

		final boolean bObtainFromOntology = preferences.getCurrentBooleanValueFor(QuestPreferences.OBTAIN_FROM_ONTOLOGY);
		final boolean bObtainFromMappings = preferences.getCurrentBooleanValueFor(QuestPreferences.OBTAIN_FROM_MAPPINGS);
		final String unfoldingMode = (String) preferences.getCurrentValue(QuestPreferences.ABOX_MODE);

		// pm.reasonerTaskStarted("Classifying...");
		// pm.reasonerTaskBusy();

		questInstance = new Quest(translatedOntologyMerge, obdaModel, preferences);

		if(this.applyUserConstraints)
			questInstance.setImplicitDBConstraints(userConstraints);

		//if( this.applyExcludeFromTMappings )
			questInstance.setExcludeFromTMappings(this.excludeFromTMappings);

		Set<OWLOntology> importsClosure = man.getImportsClosure(getRootOntology());


		try {
			// pm.reasonerTaskProgressChanged(1, 4);

			// Setup repository
			questInstance.setupRepository();
			// pm.reasonerTaskProgressChanged(2, 4);

			// Retrives the connection from Quest
			//conn = questInstance.getConnection();
			//conn = questInstance.getNonPoolConnection();
			owlconn = new QuestOWLConnection(conn);
			// pm.reasonerTaskProgressChanged(3, 4);


			questready = true;
			log.debug("Quest has completed the setup and it is ready for query answering!");
		} catch (Exception e) {
			throw e;
		} finally {
			// pm.reasonerTaskProgressChanged(4, 4);
			// pm.reasonerTaskStopped();
		}
	}

	@Override
	public void dispose() {
		super.dispose();
		try {
			conn.close();
		} catch (Exception e) {
			log.debug(e.getMessage());
		}

		try {
			questInstance.dispose();
		} catch (Exception e) {
			log.debug(e.getMessage());
		}
	}

	/***
	 * This method loads the given ontologies in the system. This will merge
	 * these new ontologies with the existing ones in a set. Then it will
	 * translate the assertions in all the ontologies into a single one, in our
	 * internal representation.
	 *
	 * The translation is done using our OWLAPITranslator that gets the TBox
	 * part of the ontologies and filters all the DL-Lite axioms (RDFS/OWL2QL
	 * and DL-Lite).
	 *
	 * The original ontologies and the merged/translated ontology are kept and
	 * are used later when classify() is called.
	 *
	 */
	public static Ontology loadOntologies(OWLOntology ontology) throws Exception {
		/*
		 * We will keep track of the loaded ontologies and translate the TBox
		 * part of them into our internal representation.
		 */
		log.debug("Load ontologies called. Translating ontologies.");

		try {
			Ontology mergeOntology = OWLAPI3TranslatorUtility.translateImportsClosure(ontology);
			return mergeOntology;
		} catch (Exception e) {
			throw e;
		}
//		log.debug("Ontology loaded: {}", mergeOntology);
	}



	public QuestOWLConnection getConnection() throws OBDAException {
		return owlconn;
	}

	/**
	 * Replaces the owl connection with a new one
	 * Called when the user cancels a query. Easier to get a new connection, than waiting for the cancel
	 * @return The old connection: The caller must close this connection
	 * @throws OBDAException
	 */
	public QuestOWLConnection replaceConnection() throws OBDAException {
		QuestOWLConnection oldconn = this.owlconn;
		conn = questInstance.getNonPoolConnection();
		owlconn = new QuestOWLConnection(conn);
		return oldconn;
	}

	@Override
	public String getReasonerName() {
		return "Quest";
	}

	@Override
	public FreshEntityPolicy getFreshEntityPolicy() {
		return structuralReasoner.getFreshEntityPolicy();
	}

	/**
	 * Gets the IndividualNodeSetPolicy in use by this reasoner. The policy is
	 * set at reasoner creation time.
	 *
	 * @return The policy.
	 */
	@Override
	public IndividualNodeSetPolicy getIndividualNodeSetPolicy() {
		return structuralReasoner.getIndividualNodeSetPolicy();
	}

	/**
	 * Gets the version of this reasoner.
	 *
	 * @return The version of this reasoner. Not <code>null</code>.
	 */
	public Version getReasonerVersion() {
		return version;
	}

	@Override
	protected void handleChanges(Set<OWLAxiom> addAxioms, Set<OWLAxiom> removeAxioms) {
		prepared = false;
		prepareReasoner();
	}

	public void interrupt() {
		interrupted = true;
	}

	private void ensurePrepared() {
		if (!prepared) {
			prepareReasoner();
		}
	}

	public void prepareReasoner() throws ReasonerInterruptedException, TimeOutException {

		pm.reasonerTaskStarted("Classifying...");
		pm.reasonerTaskBusy();
		structuralReasoner.prepareReasoner();
		try {
			/*
			 * Compute the an ontology with the merge of all the vocabulary and
			 * axioms of the closure of the root ontology
			 */
			this.translatedOntologyMerge = loadOntologies(getRootOntology());

			questready = false;
			questException = null;
			errorMessage = "";
			try {
				prepareQuestInstance();
				questready = true;
				questException = null;
				errorMessage = "";
			} catch (Exception e) {
				questready = false;
				questException = e;
				errorMessage = e.getMessage();
				log.error("Could not initialize the Quest query answering engine. Answering queries will not be available.");
				log.error(e.getMessage(), e);
				throw e;
			}
		} catch (Exception e) {
			throw new ReasonerInternalException(e);
		} finally {
			prepared = true;
			pm.reasonerTaskStopped();
		}

	}

	public void precomputeInferences(InferenceType... inferenceTypes) throws ReasonerInterruptedException, TimeOutException,
			InconsistentOntologyException {
		// System.out.println("precomputeInferences");
		ensurePrepared();
		// prepareReasoner();
	}

	public boolean isPrecomputed(InferenceType inferenceType) {
		// return true;
		return prepared;
	}

	public Set<InferenceType> getPrecomputableInferenceTypes() {
		return structuralReasoner.getPrecomputableInferenceTypes();
	}

	private void throwExceptionIfInterrupted() {
		if (interrupted) {
			interrupted = false;
			throw new ReasonerInterruptedException();
		}
	}

	// ////////////////////////////////////////////////////////////////////////////////////////////////////

	public boolean isConsistent() {
		return true;
	}

	//info to return which axiom was inconsistent during the check
	public Object getInconsistentAxiom() {
		return inconsistent;
	}

	public boolean isQuestConsistent() throws ReasonerInterruptedException, TimeOutException {
		return isDisjointAxiomsConsistent() && isFunctionalPropertyAxiomsConsistent();
	}

	private boolean isDisjointAxiomsConsistent() throws ReasonerInterruptedException, TimeOutException {

		//deal with disjoint classes
		{
			final String strQueryClass = "ASK {?x a <%s>; a <%s> }";

			for (NaryAxiom<ClassExpression> dda : translatedOntologyMerge.getDisjointClassesAxioms()) {
				// TODO: handle complex class expressions and many pairs of disjoint classes
				Collection<ClassExpression> disj = dda.getComponents();
				Iterator<ClassExpression> classIterator = disj.iterator();
				ClassExpression s1 = classIterator.next();
				ClassExpression s2 = classIterator.next();
				String strQuery = String.format(strQueryClass, s1, s2);

				boolean isConsistent = executeConsistencyQuery(strQuery);
				if (!isConsistent) {
					inconsistent = dda;
					return false;
				}
			}
		}

		//deal with disjoint properties
		{
			final String strQueryProp = "ASK {?x <%s> ?y; <%s> ?y }";

			for(NaryAxiom<ObjectPropertyExpression> dda
						: translatedOntologyMerge.getDisjointObjectPropertiesAxioms()) {
				// TODO: handle role inverses and multiple arguments
				Collection<ObjectPropertyExpression> props = dda.getComponents();
				Iterator<ObjectPropertyExpression> iterator = props.iterator();
				ObjectPropertyExpression p1 = iterator.next();
				ObjectPropertyExpression p2 = iterator.next();
				String strQuery = String.format(strQueryProp, p1, p2);

				boolean isConsistent = executeConsistencyQuery(strQuery);
				if (!isConsistent) {
					inconsistent = dda;
					return false;
				}
			}
		}

		{
			final String strQueryProp = "ASK {?x <%s> ?y; <%s> ?y }";

			for(NaryAxiom<DataPropertyExpression> dda
						: translatedOntologyMerge.getDisjointDataPropertiesAxioms()) {
				// TODO: handle role inverses and multiple arguments
				Collection<DataPropertyExpression> props = dda.getComponents();
				Iterator<DataPropertyExpression> iterator = props.iterator();
				DataPropertyExpression p1 = iterator.next();
				DataPropertyExpression p2 = iterator.next();
				String strQuery = String.format(strQueryProp, p1, p2);

				boolean isConsistent = executeConsistencyQuery(strQuery);
				if (!isConsistent) {
					inconsistent = dda;
					return false;
				}
			}
		}

		return true;
	}

	private boolean isFunctionalPropertyAxiomsConsistent() throws ReasonerInterruptedException, TimeOutException {

		//deal with functional properties

		final String strQueryFunc = "ASK { ?x <%s> ?y; <%s> ?z. FILTER (?z != ?y) }";

		for (ObjectPropertyExpression pfa : translatedOntologyMerge.getFunctionalObjectProperties()) {
			// TODO: handle inverses
			String propFunc = pfa.getName();
			String strQuery = String.format(strQueryFunc, propFunc, propFunc);

			boolean isConsistent = executeConsistencyQuery(strQuery);
			if (!isConsistent) {
				inconsistent = pfa;
				return false;
			}
		}

		for (DataPropertyExpression pfa : translatedOntologyMerge.getFunctionalDataProperties()) {
			String propFunc = pfa.getName();
			String strQuery = String.format(strQueryFunc, propFunc, propFunc);

			boolean isConsistent = executeConsistencyQuery(strQuery);
			if (!isConsistent) {
				inconsistent = pfa;
				return false;
			}
		}

		return true;
	}

	private boolean executeConsistencyQuery(String strQuery) {
		QuestStatement query;
		try {
			query = conn.createStatement();
			ResultSet rs = query.execute(strQuery);
			TupleResultSet trs = ((TupleResultSet)rs);
			if (trs!= null && trs.nextRow()){
				String value = trs.getConstant(0).getValue();
				boolean b = Boolean.parseBoolean(value);
				trs.close();
				if (b)
					return false;
			}

		} catch (OBDAException e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public boolean isSatisfiable(@Nonnull OWLClassExpression classExpression) throws ReasonerInterruptedException, TimeOutException,
			ClassExpressionNotInProfileException, FreshEntitiesException, InconsistentOntologyException {
		return structuralReasoner.isSatisfiable(classExpression);
	}

	@Nonnull
	@Override
	public Node<OWLClass> getUnsatisfiableClasses() throws ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getUnsatisfiableClasses();
	}

	@Override
	public boolean isEntailed(@Nonnull OWLAxiom axiom) throws ReasonerInterruptedException, UnsupportedEntailmentTypeException, TimeOutException,
			AxiomNotInProfileException, FreshEntitiesException, InconsistentOntologyException {
		return structuralReasoner.isEntailed(axiom);
	}

	@Override
	public boolean isEntailed(@Nonnull Set<? extends OWLAxiom> axioms) throws ReasonerInterruptedException, UnsupportedEntailmentTypeException,
			TimeOutException, AxiomNotInProfileException, FreshEntitiesException, InconsistentOntologyException {
		return structuralReasoner.isEntailed(axioms);
	}

	@Override
	public boolean isEntailmentCheckingSupported(@Nonnull AxiomType<?> axiomType) {
		return structuralReasoner.isEntailmentCheckingSupported(axiomType);
	}

	@Nonnull
	@Override
	public Node<OWLClass> getTopClassNode() {
		return structuralReasoner.getTopClassNode();
	}

	@Nonnull
	@Override
	public Node<OWLClass> getBottomClassNode() {
		return structuralReasoner.getBottomClassNode();
	}

	@Nonnull
	@Override
	public NodeSet<OWLClass> getSubClasses(@Nonnull OWLClassExpression ce, boolean direct) throws InconsistentOntologyException,
			ClassExpressionNotInProfileException, FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getSubClasses(ce, direct);
	}

	@Nonnull
	@Override
	public NodeSet<OWLClass> getSuperClasses(@Nonnull OWLClassExpression ce, boolean direct) throws InconsistentOntologyException,
			ClassExpressionNotInProfileException, FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getSuperClasses(ce, direct);
	}

	@Nonnull
	@Override
	public Node<OWLClass> getEquivalentClasses(@Nonnull OWLClassExpression ce) throws InconsistentOntologyException,
			ClassExpressionNotInProfileException, FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getEquivalentClasses(ce);
	}

	@Nonnull
	@Override
	public NodeSet<OWLClass> getDisjointClasses(@Nonnull OWLClassExpression ce) {
		return structuralReasoner.getDisjointClasses(ce);
	}

	@Nonnull
	@Override
	public Node<OWLObjectPropertyExpression> getTopObjectPropertyNode() {
		return structuralReasoner.getTopObjectPropertyNode();
	}

	@Nonnull
	@Override
	public Node<OWLObjectPropertyExpression> getBottomObjectPropertyNode() {
		return structuralReasoner.getBottomObjectPropertyNode();
	}

	@Nonnull
	@Override
	public NodeSet<OWLObjectPropertyExpression> getSubObjectProperties(@Nonnull OWLObjectPropertyExpression pe, boolean direct)
			throws InconsistentOntologyException, FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getSubObjectProperties(pe, direct);
	}

	@Nonnull
	@Override
	public NodeSet<OWLObjectPropertyExpression> getSuperObjectProperties(@Nonnull OWLObjectPropertyExpression pe, boolean direct)
			throws InconsistentOntologyException, FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getSuperObjectProperties(pe, direct);
	}

	@Nonnull
	@Override
	public Node<OWLObjectPropertyExpression> getEquivalentObjectProperties(@Nonnull OWLObjectPropertyExpression pe)
			throws InconsistentOntologyException, FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getEquivalentObjectProperties(pe);
	}

	@Nonnull
	@Override
	public NodeSet<OWLObjectPropertyExpression> getDisjointObjectProperties(@Nonnull OWLObjectPropertyExpression pe)
			throws InconsistentOntologyException, FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getDisjointObjectProperties(pe);
	}

	@Nonnull
	@Override
	public Node<OWLObjectPropertyExpression> getInverseObjectProperties(@Nonnull OWLObjectPropertyExpression pe)
			throws InconsistentOntologyException, FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getInverseObjectProperties(pe);
	}

	@Nonnull
	@Override
	public NodeSet<OWLClass> getObjectPropertyDomains(@Nonnull OWLObjectPropertyExpression pe, boolean direct) throws InconsistentOntologyException,
			FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getObjectPropertyDomains(pe, direct);
	}

	@Nonnull
	@Override
	public NodeSet<OWLClass> getObjectPropertyRanges(@Nonnull OWLObjectPropertyExpression pe, boolean direct) throws InconsistentOntologyException,
			FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getObjectPropertyRanges(pe, direct);
	}

	@Nonnull
	@Override
	public Node<OWLDataProperty> getTopDataPropertyNode() {
		return structuralReasoner.getTopDataPropertyNode();
	}

	@Nonnull
	@Override
	public Node<OWLDataProperty> getBottomDataPropertyNode() {
		return structuralReasoner.getBottomDataPropertyNode();
	}

	@Nonnull
	@Override
	public NodeSet<OWLDataProperty> getSubDataProperties(@Nonnull OWLDataProperty pe, boolean direct) throws InconsistentOntologyException,
			FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getSubDataProperties(pe, direct);
	}

	@Nonnull
	@Override
	public NodeSet<OWLDataProperty> getSuperDataProperties(@Nonnull OWLDataProperty pe, boolean direct) throws InconsistentOntologyException,
			FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getSuperDataProperties(pe, direct);
	}

	@Nonnull
	@Override
	public Node<OWLDataProperty> getEquivalentDataProperties(@Nonnull OWLDataProperty pe) throws InconsistentOntologyException,
			FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getEquivalentDataProperties(pe);
	}

	@Nonnull
	@Override
	public NodeSet<OWLDataProperty> getDisjointDataProperties(@Nonnull OWLDataPropertyExpression pe) throws InconsistentOntologyException,
			FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getDisjointDataProperties(pe);
	}

	@Nonnull
	@Override
	public NodeSet<OWLClass> getDataPropertyDomains(@Nonnull OWLDataProperty pe, boolean direct) throws InconsistentOntologyException,
			FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getDataPropertyDomains(pe, direct);
	}

	@Nonnull
	@Override
	public NodeSet<OWLClass> getTypes(@Nonnull OWLNamedIndividual ind, boolean direct) throws InconsistentOntologyException, FreshEntitiesException,
			ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getTypes(ind, direct);
	}

	@Nonnull
	@Override
	public NodeSet<OWLNamedIndividual> getInstances(@Nonnull OWLClassExpression ce, boolean direct) throws InconsistentOntologyException,
			ClassExpressionNotInProfileException, FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getInstances(ce, direct);
	}

	@Nonnull
	@Override
	public NodeSet<OWLNamedIndividual> getObjectPropertyValues(@Nonnull OWLNamedIndividual ind, @Nonnull OWLObjectPropertyExpression pe)
			throws InconsistentOntologyException, FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getObjectPropertyValues(ind, pe);
	}


	@Nonnull
	@Override
	public Set<OWLLiteral> getDataPropertyValues(@Nonnull OWLNamedIndividual ind, @Nonnull OWLDataProperty pe) throws InconsistentOntologyException,
			FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getDataPropertyValues(ind, pe);
	}

	@Nonnull
	@Override
	public Node<OWLNamedIndividual> getSameIndividuals(@Nonnull OWLNamedIndividual ind) throws InconsistentOntologyException,
			FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getSameIndividuals(ind);
	}

	@Nonnull
	@Override
	public NodeSet<OWLNamedIndividual> getDifferentIndividuals(@Nonnull OWLNamedIndividual ind) throws InconsistentOntologyException,
			FreshEntitiesException, ReasonerInterruptedException, TimeOutException {
		return structuralReasoner.getDifferentIndividuals(ind);
	}

	/**
	 * Methods to get the empty concepts and roles in the ontology using the given mappings.
	 * It generates SPARQL queries to check for entities.
	 * @return QuestOWLEmptyEntitiesChecker class to get empty concepts and roles
	 * @throws Exception
	 */

	public QuestOWLEmptyEntitiesChecker getEmptyEntitiesChecker() throws Exception{
		QuestOWLEmptyEntitiesChecker empties = new QuestOWLEmptyEntitiesChecker(translatedOntologyMerge, owlconn);
		return empties;
	}

	protected OWLDataFactory getDataFactory() {
		return getRootOntology().getOWLOntologyManager().getOWLDataFactory();
	}

	public void dumpClassHierarchy(boolean showBottomNode) {
		dumpClassHierarchy(OWLClassNode.getTopNode(), 0, showBottomNode);
	}

	private void dumpClassHierarchy(Node<OWLClass> cls, int level, boolean showBottomNode) {
		if (!showBottomNode && cls.isBottomNode()) {
			return;
		}
		printIndent(level);
		OWLClass representative = cls.getRepresentativeElement();
//		System.out.println(getEquivalentClasses(representative));
		for (Node<OWLClass> subCls : getSubClasses(representative, true)) {
			dumpClassHierarchy(subCls, level + 1, showBottomNode);
		}
	}

	public void dumpObjectPropertyHierarchy(boolean showBottomNode) {
		dumpObjectPropertyHierarchy(OWLObjectPropertyNode.getTopNode(), 0, showBottomNode);
	}

	private void dumpObjectPropertyHierarchy(Node<OWLObjectPropertyExpression> cls, int level, boolean showBottomNode) {
		if (!showBottomNode && cls.isBottomNode()) {
			return;
		}
		printIndent(level);
		OWLObjectPropertyExpression representative = cls.getRepresentativeElement();
//		System.out.println(getEquivalentObjectProperties(representative));
		for (Node<OWLObjectPropertyExpression> subProp : getSubObjectProperties(representative, true)) {
			dumpObjectPropertyHierarchy(subProp, level + 1, showBottomNode);
		}
	}

	public void dumpDataPropertyHierarchy(boolean showBottomNode) {
		dumpDataPropertyHierarchy(OWLDataPropertyNode.getTopNode(), 0, showBottomNode);
	}

	private void dumpDataPropertyHierarchy(Node<OWLDataProperty> cls, int level, boolean showBottomNode) {
		if (!showBottomNode && cls.isBottomNode()) {
			return;
		}
		printIndent(level);
		OWLDataProperty representative = cls.getRepresentativeElement();
//		System.out.println(getEquivalentDataProperties(representative));
		for (Node<OWLDataProperty> subProp : getSubDataProperties(representative, true)) {
			dumpDataPropertyHierarchy(subProp, level + 1, showBottomNode);
		}
	}

	private void printIndent(int level) {
		for (int i = 0; i < level; i++) {
			System.out.print("    ");
		}
	}

	@Override
	public void close() throws Exception {
		dispose();
	}

	// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// ////
	// //// HierarchyInfo
	// ////
	// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private abstract class HierarchyInfo<T extends OWLObject> {

		private RawHierarchyProvider<T> rawParentChildProvider;

		/**
		 * The entity that always appears in the top node in the hierarchy
		 */
		private T topEntity;

		/**
		 * The entity that always appears as the bottom node in the hierarchy
		 */
		private T bottomEntity;

		private Set<T> directChildrenOfTopNode = new HashSet<T>();

		private Set<T> directParentsOfBottomNode = new HashSet<T>();

		private NodeCache<T> nodeCache;

		private String name;

		private int classificationSize;

		public HierarchyInfo(String name, T topEntity, T bottomEntity, RawHierarchyProvider<T> rawParentChildProvider) {
			this.topEntity = topEntity;
			this.bottomEntity = bottomEntity;
			this.nodeCache = new NodeCache<T>(this);
			this.rawParentChildProvider = rawParentChildProvider;
			this.name = name;
		}

		public RawHierarchyProvider<T> getRawParentChildProvider() {
			return rawParentChildProvider;
		}

		/**
		 * Gets the set of relevant entities from the specified ontology
		 *
		 * @param ont
		 *            The ontology
		 * @return A set of entities to be "classified"
		 */
		protected abstract Set<T> getEntities(OWLOntology ont);

		/**
		 * Creates a node for a given set of entities
		 *
		 * @param cycle
		 *            The set of entities
		 * @return A node
		 */
		protected abstract DefaultNode<T> createNode(Set<T> cycle);

		protected abstract DefaultNode<T> createNode();

		/**
		 * Gets the set of relevant entities in a particular axiom
		 *
		 * @param ax
		 *            The axiom
		 * @return The set of relevant entities in the signature of the
		 *         specified axiom
		 */
		protected abstract Set<? extends T> getEntitiesInSignature(OWLAxiom ax);

		private Set<T> getEntitiesInSignature(Set<OWLAxiom> axioms) {
			Set<T> result = new HashSet<T>();
			for (OWLAxiom ax : axioms) {
				result.addAll(getEntitiesInSignature(ax));
			}
			return result;
		}

		public void computeHierarchy() {
			// pm.reasonerTaskStarted("Computing " + name + " hierarchy");
			// pm.reasonerTaskBusy();
			nodeCache.clear();
			Map<T, Collection<T>> cache = new HashMap<T, Collection<T>>();
			Set<T> entities = new HashSet<T>();
			for (OWLOntology ont : getRootOntology().getImportsClosure()) {
				entities.addAll(getEntities(ont));
			}
			classificationSize = entities.size();
			// pm.reasonerTaskProgressChanged(0, classificationSize);
			updateForSignature(entities, cache);
			// pm.reasonerTaskStopped();
		}

		private void updateForSignature(Set<T> signature, Map<T, Collection<T>> cache) {
			HashSet<Set<T>> cyclesResult = new HashSet<Set<T>>();
			Set<T> processed = new HashSet<T>();
			nodeCache.clearTopNode();
			nodeCache.clearBottomNode();
			nodeCache.clearNodes(signature);

			directChildrenOfTopNode.removeAll(signature);

			Set<T> equivTopOrChildrenOfTop = new HashSet<T>();
			Set<T> equivBottomOrParentsOfBottom = new HashSet<T>();
			for (T entity : signature) {
				if (!processed.contains(entity)) {
					// pm.reasonerTaskProgressChanged(processed.size(),
					// signature.size());
					tarjan(entity, 0, new Stack<T>(), new HashMap<T, Integer>(), new HashMap<T, Integer>(), cyclesResult, processed,
							new HashSet<T>(), cache, equivTopOrChildrenOfTop, equivBottomOrParentsOfBottom);
					throwExceptionIfInterrupted();
				}
			}
			// Store new cycles
			for (Set<T> cycle : cyclesResult) {
				nodeCache.addNode(cycle);
			}

			directChildrenOfTopNode.addAll(equivTopOrChildrenOfTop);
			directChildrenOfTopNode.removeAll(nodeCache.getTopNode().getEntities());

			directParentsOfBottomNode.addAll(equivBottomOrParentsOfBottom);
			directParentsOfBottomNode.removeAll(nodeCache.getBottomNode().getEntities());

			// Now check that each found cycle has a proper parent an child
			for (Set<T> node : cyclesResult) {
				if (!node.contains(topEntity) && !node.contains(bottomEntity)) {
					boolean childOfTop = true;
					for (T element : node) {
						Collection<T> parents = rawParentChildProvider.getParents(element);
						parents.removeAll(node);
						parents.removeAll(nodeCache.getTopNode().getEntities());
						if (!parents.isEmpty()) {
							childOfTop = false;
							break;
						}
					}
					if (childOfTop) {
						directChildrenOfTopNode.addAll(node);
					}

					boolean parentOfBottom = true;
					for (T element : node) {
						Collection<T> children = rawParentChildProvider.getChildren(element);
						children.removeAll(node);
						children.removeAll(nodeCache.getBottomNode().getEntities());
						if (!children.isEmpty()) {
							parentOfBottom = false;
							break;
						}
					}
					if (parentOfBottom) {
						directParentsOfBottomNode.addAll(node);
					}
				}

			}

		}

		/**
		 * Processes the specified signature that represents the signature of
		 * potential changes
		 *
		 * @param signature
		 *            The signature
		 */
		public void processChanges(Set<T> signature, Set<OWLAxiom> added, Set<OWLAxiom> removed) {
			updateForSignature(signature, null);
		}

		// ////////////////////////////////////////////////////////////////////////////////////////////////////////
		// ////////////////////////////////////////////////////////////////////////////////////////////////////////

		/**
		 * Applies the tarjan algorithm for a given entity. This computes the
		 * cycle that the entity is involved in (if any).
		 *
		 * @param entity
		 *            The entity
		 * @param cache
		 *            A cache of children to parents - may be <code>null</code>
		 *            if no caching is to take place.
		 * @param childrenOfTop
		 *            A set of entities that have a raw parent that is the top
		 *            entity
		 * @param parentsOfBottom
		 *            A set of entities that have a raw parent that is the
		 *            bottom entity
		 */
		public void tarjan(T entity, int index, Stack<T> stack, Map<T, Integer> indexMap, Map<T, Integer> lowlinkMap, Set<Set<T>> result,
				Set<T> processed, Set<T> stackEntities, Map<T, Collection<T>> cache, Set<T> childrenOfTop, Set<T> parentsOfBottom) {
			throwExceptionIfInterrupted();
			if (processed.add(entity)) {
				Collection<T> rawChildren = rawParentChildProvider.getChildren(entity);
				if (rawChildren.isEmpty() || rawChildren.contains(bottomEntity)) {
					parentsOfBottom.add(entity);
				}
			}
			// pm.reasonerTaskProgressChanged(processed.size(),
			// classificationSize);
			indexMap.put(entity, index);
			lowlinkMap.put(entity, index);
			index = index + 1;
			stack.push(entity);
			stackEntities.add(entity);

			// Get the raw parents - cache if necessary
			Collection<T> rawParents = null;
			if (cache != null) {
				// We are therefore caching raw parents of children.
				rawParents = cache.get(entity);
				if (rawParents == null) {
					// Not in cache!
					rawParents = rawParentChildProvider.getParents(entity);
					// Note down if our entity is a
					if (rawParents.isEmpty() || rawParents.contains(topEntity)) {
						childrenOfTop.add(entity);
					}
					cache.put(entity, rawParents);

				}
			} else {
				rawParents = rawParentChildProvider.getParents(entity);
				// Note down if our entity is a
				if (rawParents.isEmpty() || rawParents.contains(topEntity)) {
					childrenOfTop.add(entity);
				}
			}

			for (T superEntity : rawParents) {
				if (!indexMap.containsKey(superEntity)) {
					tarjan(superEntity, index, stack, indexMap, lowlinkMap, result, processed, stackEntities, cache, childrenOfTop,
							parentsOfBottom);
					lowlinkMap.put(entity, Math.min(lowlinkMap.get(entity), lowlinkMap.get(superEntity)));
				} else if (stackEntities.contains(superEntity)) {
					lowlinkMap.put(entity, Math.min(lowlinkMap.get(entity), indexMap.get(superEntity)));
				}
			}
			if (lowlinkMap.get(entity).equals(indexMap.get(entity))) {
				Set<T> scc = new HashSet<T>();
				while (true) {
					T clsPrime = stack.pop();
					stackEntities.remove(clsPrime);
					scc.add(clsPrime);
					if (clsPrime.equals(entity)) {
						break;
					}
				}
				if (scc.size() > 1) {
					// We ADD a cycle
					result.add(scc);
				}
			}
		}

		// ////////////////////////////////////////////////////////////////////////////////////////////////////////
		// ////////////////////////////////////////////////////////////////////////////////////////////////////////

		public NodeSet<T> getNodeHierarchyChildren(T parent, boolean direct, DefaultNodeSet<T> ns) {
			Node<T> node = nodeCache.getNode(parent);

			if (node.isBottomNode()) {
				return ns;
			}

			Set<T> directChildren = new HashSet<T>();
			for (T equiv : node) {
				directChildren.addAll(rawParentChildProvider.getChildren(equiv));
				if (directParentsOfBottomNode.contains(equiv)) {
					ns.addNode(nodeCache.getBottomNode());
				}
			}
			directChildren.removeAll(node.getEntities());

			if (node.isTopNode()) {
				// Special treatment
				directChildren.addAll(directChildrenOfTopNode);
			}

			for (Node<T> childNode : nodeCache.getNodes(directChildren)) {
				ns.addNode(childNode);
			}

			if (!direct) {
				for (T child : directChildren) {
					getNodeHierarchyChildren(child, direct, ns);
				}
			}
			return ns;
		}

		public NodeSet<T> getNodeHierarchyParents(T child, boolean direct, DefaultNodeSet<T> ns) {
			Node<T> node = nodeCache.getNode(child);

			if (node.isTopNode()) {
				return ns;
			}

			Set<T> directParents = new HashSet<T>();
			for (T equiv : node) {
				directParents.addAll(rawParentChildProvider.getParents(equiv));
				if (directChildrenOfTopNode.contains(equiv)) {
					ns.addNode(nodeCache.getTopNode());
				}
			}
			directParents.removeAll(node.getEntities());

			if (node.isBottomNode()) {
				// Special treatment
				directParents.addAll(directParentsOfBottomNode);
			}

			for (Node<T> parentNode : nodeCache.getNodes(directParents)) {
				ns.addNode(parentNode);
			}

			if (!direct) {
				for (T parent : directParents) {
					getNodeHierarchyParents(parent, direct, ns);
				}
			}
			return ns;
		}

		public Node<T> getEquivalents(T element) {
			return nodeCache.getNode(element);
		}
	}

	private static class NodeCache<T extends OWLObject> {

		private HierarchyInfo<T> hierarchyInfo;

		private Node<T> topNode;

		private Node<T> bottomNode;

		private Map<T, Node<T>> map = new HashMap<T, Node<T>>();

		protected NodeCache(HierarchyInfo<T> hierarchyInfo) {
			this.hierarchyInfo = hierarchyInfo;
			clearTopNode();
			clearBottomNode();
		}

		public void addNode(Node<T> node) {
			for (T element : node.getEntities()) {
				map.put(element, node);
				if (element.isTopEntity()) {
					topNode = node;
				} else if (element.isBottomEntity()) {
					bottomNode = node;
				}
			}
		}

		public Set<Node<T>> getNodes(Set<T> elements) {
			Set<Node<T>> result = new HashSet<Node<T>>();
			for (T element : elements) {
				result.add(getNode(element));
			}
			return result;
		}

		public Set<T> getTopEntities() {
			return topNode.getEntities();
		}

		public Set<T> getBottomEntities() {
			return bottomNode.getEntities();
		}

		public Node<T> getNode(T containing) {
			Node<T> parentNode = map.get(containing);
			if (parentNode != null) {
				return parentNode;
			} else {
				return hierarchyInfo.createNode(Collections.singleton(containing));
			}
		}

		public void addNode(Set<T> elements) {
			addNode(hierarchyInfo.createNode(elements));
		}

		public Node<T> getTopNode() {
			return topNode;
		}

		public Node<T> getBottomNode() {
			return bottomNode;
		}

		public void setTopNode(Node<T> topNode) {
			this.topNode = topNode;
		}

		public void setBottomNode(Node<T> bottomNode) {
			this.bottomNode = bottomNode;
		}

		public void clearTopNode() {
			removeNode(hierarchyInfo.topEntity);
			topNode = hierarchyInfo.createNode(Collections.singleton(hierarchyInfo.topEntity));
			addNode(topNode);
		}

		public void clearBottomNode() {
			removeNode(hierarchyInfo.bottomEntity);
			bottomNode = hierarchyInfo.createNode(Collections.singleton(hierarchyInfo.bottomEntity));
			addNode(bottomNode);
		}

		public void clearNodes(Set<T> containing) {
			for (T entity : containing) {
				removeNode(entity);
			}
		}

		public void clear() {
			map.clear();
			clearTopNode();
			clearBottomNode();
		}

		public void removeNode(T containing) {
			Node<T> node = map.remove(containing);
			if (node != null) {
				for (T object : node.getEntities()) {
					map.remove(object);
				}
			}
		}
	}

	private class ClassHierarchyInfo extends HierarchyInfo<OWLClass> {

		private ClassHierarchyInfo() {
			super("class", getDataFactory().getOWLThing(), getDataFactory().getOWLNothing(), new RawClassHierarchyProvider());
		}

		@Override
		protected Set<OWLClass> getEntitiesInSignature(OWLAxiom ax) {
			return ax.getClassesInSignature();
		}

		@Override
		protected DefaultNode<OWLClass> createNode(Set<OWLClass> cycle) {
			return new OWLClassNode(cycle);
		}

		@Override
		protected Set<OWLClass> getEntities(OWLOntology ont) {
			return ont.getClassesInSignature();
		}

		@Override
		protected DefaultNode<OWLClass> createNode() {
			return new OWLClassNode();
		}
	}

	private class ObjectPropertyHierarchyInfo extends HierarchyInfo<OWLObjectPropertyExpression> {

		private ObjectPropertyHierarchyInfo() {
			super("object property", getDataFactory().getOWLTopObjectProperty(), getDataFactory().getOWLBottomObjectProperty(),
					new RawObjectPropertyHierarchyProvider());
		}

		@Override
		protected Set<OWLObjectPropertyExpression> getEntitiesInSignature(OWLAxiom ax) {
			Set<OWLObjectPropertyExpression> result = new HashSet<OWLObjectPropertyExpression>();
			for (OWLObjectProperty property : ax.getObjectPropertiesInSignature()) {
				result.add(property);
				result.add(property.getInverseProperty());
			}
			return result;
		}

		@Override
		protected Set<OWLObjectPropertyExpression> getEntities(OWLOntology ont) {
			Set<OWLObjectPropertyExpression> result = new HashSet<OWLObjectPropertyExpression>();
			for (OWLObjectPropertyExpression property : ont.getObjectPropertiesInSignature()) {
				result.add(property);
				result.add(property.getInverseProperty());
			}
			return result;
		}

		@Override
		protected DefaultNode<OWLObjectPropertyExpression> createNode(Set<OWLObjectPropertyExpression> cycle) {
			return new OWLObjectPropertyNode(cycle);
		}

		@Override
		protected DefaultNode<OWLObjectPropertyExpression> createNode() {
			return new OWLObjectPropertyNode();
		}

		/**
		 * Processes the specified signature that represents the signature of
		 * potential changes
		 *
		 * @param signature
		 *            The signature
		 */
		@Override
		public void processChanges(Set<OWLObjectPropertyExpression> signature, Set<OWLAxiom> added, Set<OWLAxiom> removed) {
			boolean rebuild = false;
			for (OWLAxiom ax : added) {
				if (ax instanceof OWLObjectPropertyAxiom) {
					rebuild = true;
					break;
				}
			}
			if (!rebuild) {
				for (OWLAxiom ax : removed) {
					if (ax instanceof OWLObjectPropertyAxiom) {
						rebuild = true;
						break;
					}
				}
			}
			if (rebuild) {
				((RawObjectPropertyHierarchyProvider) getRawParentChildProvider()).rebuild();
			}
			super.processChanges(signature, added, removed);
		}
	}

	private class DataPropertyHierarchyInfo extends HierarchyInfo<OWLDataProperty> {

		private DataPropertyHierarchyInfo() {
			super("data property", getDataFactory().getOWLTopDataProperty(), getDataFactory().getOWLBottomDataProperty(),
					new RawDataPropertyHierarchyProvider());
		}

		@Override
		protected Set<OWLDataProperty> getEntitiesInSignature(OWLAxiom ax) {
			return ax.getDataPropertiesInSignature();
		}

		@Override
		protected Set<OWLDataProperty> getEntities(OWLOntology ont) {
			return ont.getDataPropertiesInSignature();
		}

		@Override
		protected DefaultNode<OWLDataProperty> createNode(Set<OWLDataProperty> cycle) {
			return new OWLDataPropertyNode(cycle);
		}

		@Override
		protected DefaultNode<OWLDataProperty> createNode() {
			return new OWLDataPropertyNode();
		}
	}

	// ///////////////////////////////////////////////////////////////////////////////////////////////////////////
	// ///////////////////////////////////////////////////////////////////////////////////////////////////////////
	// ///////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * An interface for objects who can provide the parents and children of some
	 * object.
	 *
	 * @param <T>
	 */
	private interface RawHierarchyProvider<T> {

		/**
		 * Gets the parents as asserted. These parents may also be children
		 * (resulting in equivalences).
		 *
		 * @param child
		 *            The child whose parents are to be retrieved
		 * @return The raw asserted parents of the specified child. If the child
		 *         does not have any parents then the empty set can be returned.
		 */
		Collection<T> getParents(T child);

		/**
		 * Gets the children as asserted
		 *
		 * @param parent
		 *            The parent whose children are to be retrieved
		 * @return The raw asserted children of the speicified parent
		 */
		Collection<T> getChildren(T parent);

	}

	private class RawClassHierarchyProvider implements RawHierarchyProvider<OWLClass> {

		public Collection<OWLClass> getParents(OWLClass child) {
			Collection<OWLClass> result = new HashSet<OWLClass>();
			for (OWLOntology ont : getRootOntology().getImportsClosure()) {
				for (OWLSubClassOfAxiom ax : ont.getSubClassAxiomsForSubClass(child)) {
					OWLClassExpression superCls = ax.getSuperClass();
					if (!superCls.isAnonymous()) {
						result.add(superCls.asOWLClass());
					} else if (superCls instanceof OWLObjectIntersectionOf) {
						OWLObjectIntersectionOf intersectionOf = (OWLObjectIntersectionOf) superCls;
						for (OWLClassExpression conjunct : intersectionOf.asConjunctSet()) {
							if (!conjunct.isAnonymous()) {
								result.add(conjunct.asOWLClass());
							}
						}
					}
				}
				for (OWLEquivalentClassesAxiom ax : ont.getEquivalentClassesAxioms(child)) {
					for (OWLClassExpression ce : ax.getClassExpressionsMinus(child)) {
						if (!ce.isAnonymous()) {
							result.add(ce.asOWLClass());
						} else if (ce instanceof OWLObjectIntersectionOf) {
							OWLObjectIntersectionOf intersectionOf = (OWLObjectIntersectionOf) ce;
							for (OWLClassExpression conjunct : intersectionOf.asConjunctSet()) {
								if (!conjunct.isAnonymous()) {
									result.add(conjunct.asOWLClass());
								}
							}
						}
					}
				}
			}
			return result;
		}

		public Collection<OWLClass> getChildren(OWLClass parent) {
			Collection<OWLClass> result = new HashSet<OWLClass>();
			for (OWLOntology ont : getRootOntology().getImportsClosure()) {
				for (OWLAxiom ax : ont.getReferencingAxioms(parent)) {
					if (ax instanceof OWLSubClassOfAxiom) {
						OWLSubClassOfAxiom sca = (OWLSubClassOfAxiom) ax;
						if (!sca.getSubClass().isAnonymous()) {
							Set<OWLClassExpression> conjuncts = sca.getSuperClass().asConjunctSet();
							if (conjuncts.contains(parent)) {
								result.add(sca.getSubClass().asOWLClass());
							}
						}
					} else if (ax instanceof OWLEquivalentClassesAxiom) {
						OWLEquivalentClassesAxiom eca = (OWLEquivalentClassesAxiom) ax;
						for (OWLClassExpression ce : eca.getClassExpressions()) {
							if (ce.containsConjunct(parent)) {
								for (OWLClassExpression sub : eca.getClassExpressions()) {
									if (!sub.isAnonymous() && !sub.equals(ce)) {
										result.add(sub.asOWLClass());
									}
								}
							}
						}
					}
				}
			}
			return result;
		}
	}

	private class RawObjectPropertyHierarchyProvider implements RawHierarchyProvider<OWLObjectPropertyExpression> {

		private OWLObjectPropertyManager propertyManager;

		private Map<OWLObjectPropertyExpression, Set<OWLObjectPropertyExpression>> sub2Super;

		private Map<OWLObjectPropertyExpression, Set<OWLObjectPropertyExpression>> super2Sub;

		private RawObjectPropertyHierarchyProvider() {
			rebuild();
		}

		public void rebuild() {
			propertyManager = new OWLObjectPropertyManager(getRootOntology().getOWLOntologyManager(), getRootOntology());
			sub2Super = propertyManager.getPropertyHierarchy();
			super2Sub = new HashMap<OWLObjectPropertyExpression, Set<OWLObjectPropertyExpression>>();
			for (OWLObjectPropertyExpression sub : sub2Super.keySet()) {
				for (OWLObjectPropertyExpression superProp : sub2Super.get(sub)) {
					Set<OWLObjectPropertyExpression> subs = super2Sub.get(superProp);
					if (subs == null) {
						subs = new HashSet<OWLObjectPropertyExpression>();
						super2Sub.put(superProp, subs);
					}
					subs.add(sub);
				}
			}
		}

		public Collection<OWLObjectPropertyExpression> getParents(OWLObjectPropertyExpression child) {
			if (child.isBottomEntity()) {
				return Collections.emptySet();
			}
			Set<OWLObjectPropertyExpression> propertyExpressions = sub2Super.get(child);
			if (propertyExpressions == null) {
				return Collections.emptySet();
			} else {
				return new HashSet<OWLObjectPropertyExpression>(propertyExpressions);
			}

		}

		public Collection<OWLObjectPropertyExpression> getChildren(OWLObjectPropertyExpression parent) {
			if (parent.isTopEntity()) {
				return Collections.emptySet();
			}
			Set<OWLObjectPropertyExpression> propertyExpressions = super2Sub.get(parent);
			if (propertyExpressions == null) {
				return Collections.emptySet();
			} else {
				return new HashSet<OWLObjectPropertyExpression>(propertyExpressions);
			}

		}
	}

	private class RawDataPropertyHierarchyProvider implements RawHierarchyProvider<OWLDataProperty> {

		public Collection<OWLDataProperty> getParents(OWLDataProperty child) {
			Set<OWLDataProperty> properties = new HashSet<OWLDataProperty>();
			//TODO fix?
			//for (OWLDataPropertyExpression prop : child.getSuperProperties(getRootOntology().getImportsClosure())) {
			//	properties.add(prop.asOWLDataProperty());
			//}
			return properties;
		}

		public Collection<OWLDataProperty> getChildren(OWLDataProperty parent) {
			Set<OWLDataProperty> properties = new HashSet<OWLDataProperty>();
			//TODO fix?
			//for (OWLDataPropertyExpression prop : parent.getSubProperties(getRootOntology().getImportsClosure())) {
			//	properties.add(prop.asOWLDataProperty());
			//}
			return properties;
		}
	}

	public StrabonStatement createStrabonStatement(NodeSelectivityEstimator nse) {
		StrabonStatement st = new StrabonStatement(this.questInstance, null,
				null, nse);
		//st.setFetchSize(400);
		return st;
	}

}
