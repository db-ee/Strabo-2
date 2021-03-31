package sesameWrapper;

/*
 * #%L
 * ontop-quest-sesame
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

import com.jayway.jsonpath.DocumentContext;
import it.unibz.krdb.obda.sesame.SesameHelper;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.BindingImpl;

import java.util.*;

public class StrabonLivyBindingSet implements BindingSet {

    private final ValueFactory fact = new ValueFactoryImpl();
    private static final long serialVersionUID = -8455466574395305166L;
    private DocumentContext row = null;
    private int count = 0;
    private final List<String> bindingnames;
//	private List<String> signature;

    private final SesameHelper helper = new SesameHelper();

    public StrabonLivyBindingSet(DocumentContext row, List<String> signature2) {
        this.bindingnames = signature2;
//		this.signature = signature;
        this.row = row;
        this.count = signature2.size();

    }


    @Override
    public Binding getBinding(String bindingName) {
        // return the Binding with bindingName
        return createBinding(bindingName);
    }

    @Override
    public Set<String> getBindingNames() {
        return new HashSet<String>(bindingnames);
//		try {
//
//			List<String> sign = set.getSignature();
//			Set<String> bnames = new HashSet<String>(sign.size());
//			for (String s : sign)
//				bnames.add(s);
//			return bnames;
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return null;
    }

    @Override
    public Value getValue(String bindingName) {
        return createBinding(bindingName).getValue();
    }

    private Binding createBinding(String bindingName) {
        Value value = null;

        value=fact.createLiteral(row.read(bindingName).toString());

        return new BindingImpl(bindingName, value);

//		return null;
    }

    @Override
    public boolean hasBinding(String bindingName) {
        return bindingnames.contains(bindingName);
//		try {
//			return set.getSignature().contains(bindingName);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return false;
    }

    @Override
    public Iterator<Binding> iterator() {

        List<Binding> allBindings = new LinkedList<Binding>();
        try {
            for (String s : bindingnames)
                allBindings.add(createBinding(s));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return allBindings.iterator();
    }

    @Override
    public int size() {
        return count;
    }
}
