package it.unibz.krdb.obda.owlrefplatform.core;

import it.unibz.krdb.obda.model.OBDAModel;
import it.unibz.krdb.obda.owlapi.ReformulationPlatformPreferences;

import org.semanticweb.owl.inference.OWLReasoner;
import org.semanticweb.owl.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation of the factory for creating reformulation's platform
 * reasoner
 */

public class OBDAOWLReformulationPlatformFactoryImpl implements OBDAOWLReformulationPlatformFactory {

	private OBDAModel							apic;
	private ReformulationPlatformPreferences	preferences	= null;
	private String								id;
	private String								name;
	private OWLOntologyManager					owlOntologyManager;

	private final Logger						log			= LoggerFactory.getLogger(OBDAOWLReformulationPlatformFactoryImpl.class);

	/**
	 * Sets up some prerequirements in order to create the reasoner
	 * 
	 * @param manager
	 *            the owl ontology manager
	 * @param id
	 *            the reasoner id
	 * @param name
	 *            the reasoner name
	 */
	public void setup(OWLOntologyManager manager, String id, String name) {
		this.id = id;
		this.name = name;
		this.owlOntologyManager = manager;
	}

	/**
	 * Return the current OWLOntologyManager
	 * 
	 * @return the current OWLOntologyManager
	 */
	public OWLOntologyManager getOWLOntologyManager() {
		return owlOntologyManager;
	}

	/**
	 * Returns the current reasoner id
	 * 
	 * @return the current reasoner id
	 */
	public String getReasonerId() {
		return id;
	}

	@Override
	public void setOBDAController(OBDAModel apic) {
		this.apic = apic;
	}

	@Override
	public void setPreferenceHolder(ReformulationPlatformPreferences preference) {
		this.preferences = preference;
	}

	/**
	 * Creates a new reformulation platform reasoner.
	 */
	@Override
	public OWLReasoner createReasoner(OWLOntologyManager manager) {
		return new OBDAOWLReformulationPlatform(manager);
	}

	public String getReasonerName() {
		return name;
	}

	public void initialise() throws Exception {

	}

	public void dispose() throws Exception {

	}

	/**
	 * Returns the current api controller
	 * 
	 * @return the current api controller
	 */
	public OBDAModel getApiController() {
		return apic;
	}
}
