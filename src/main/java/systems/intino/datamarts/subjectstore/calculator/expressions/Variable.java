package systems.intino.datamarts.subjectstore.calculator.expressions;

import systems.intino.datamarts.subjectstore.calculator.Expression;
import systems.intino.datamarts.subjectstore.calculator.model.vectors.DoubleVector;

import java.util.Objects;

public final class Variable implements Expression {
	private final String name;
	private DoubleVector vector;

	public Variable(String name) {
		this.name = name;
	}

	public String name() {
		return name;
	}

	public void set(DoubleVector vector) {
		this.vector = vector;
	}

	@Override
	public DoubleVector evaluate() {
		return vector;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) return true;
		if (obj == null || obj.getClass() != this.getClass()) return false;
		var that = (Variable) obj;
		return Objects.equals(this.name, that.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name);
	}

	@Override
	public String toString() {
		return "Var[" + name + ']';
	}

}
