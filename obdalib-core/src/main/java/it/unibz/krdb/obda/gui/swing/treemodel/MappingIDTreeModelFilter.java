package it.unibz.krdb.obda.gui.swing.treemodel;

import it.unibz.krdb.obda.model.OBDAMappingAxiom;

public class MappingIDTreeModelFilter extends TreeModelFilter<OBDAMappingAxiom> {

  public MappingIDTreeModelFilter() {
    super.bNegation = false;
  }

	@Override
	public boolean match(OBDAMappingAxiom object) {

	  boolean isMatch = false;

		String[] vecKeyword = strFilter.split(" ");
		for (String keyword : vecKeyword) {
		  isMatch = match(keyword, object.getId());
		  if(isMatch) {
		    break;  // end loop if a match is found!
		  }
		}
		// no match found!
    return (bNegation ? !isMatch : isMatch);
	}

  /** A helper method to check a match */
	public static boolean match(String keyword, String mappingId) {

	  if (mappingId.indexOf(keyword) != -1) {  // match found!
      return true;
    }
	  return false;
	}
}
