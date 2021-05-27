package madgik.exareme.master.queryProcessor.sparql;

import it.unibz.krdb.obda.model.Function;
import it.unibz.krdb.obda.model.Variable;

public class SpatialSelection {

    private Function function;
    private Variable var; //the var involved in selection
    private double selectivity;

    public SpatialSelection(Function function, Variable var) {
        this.var = var;
        this.function = function;
        this.selectivity = 0.01;
    }

    public Function getFunction() {
        return function;
    }

    public void setFunction(Function function) {
        this.function = function;
    }

    public Variable getVar() {
        return var;
    }

    public void setVar(Variable var) {
        this.var = var;
    }

    public double getSelectivity() {
        return selectivity;
    }

    public void setSelectivity(double selectivity) {
        this.selectivity = selectivity;
    }
}
