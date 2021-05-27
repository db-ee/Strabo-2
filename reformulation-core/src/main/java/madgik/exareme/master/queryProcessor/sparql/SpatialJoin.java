package madgik.exareme.master.queryProcessor.sparql;

import it.unibz.krdb.obda.model.Function;
import it.unibz.krdb.obda.model.Variable;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;

public class SpatialJoin {

    private Function function;
    private Variable var1;
    private Variable var2;
    private double selectivity;
    private Column col1;
    private Column col2;

    public SpatialJoin(Function function, Variable var1, Variable var2) {
        this.var1 = var1;
        this.var2 = var2;
        this.function = function;
        this.selectivity = 0.01;
    }

    public Function getFunction() {
        return function;
    }

    public void setFunction(Function function) {
        this.function = function;
    }

    public Variable getVar1() {
        return var1;
    }

    public void setVar1(Variable var1) {
        this.var1 = var1;
    }

    public Variable getVar2() {
        return var2;
    }

    public void setVar2(Variable var2) {
        this.var2 = var2;
    }

    public double getSelectivity() {
        return selectivity;
    }

    public void setSelectivity(double selectivity) {
        this.selectivity = selectivity;
    }

    public void setColumn1(Column newCol) {
        this.col1 = newCol;
    }

    public void setColumn2(Column newCol) {
        this.col2 = newCol;
    }

    public Column getCol1() {
        return col1;
    }

    public Column getCol2() {
        return col2;
    }
}
