package systems.intino.datamarts.subjectstore.calculator.expressions;

import systems.intino.datamarts.subjectstore.calculator.Expression;
import systems.intino.datamarts.subjectstore.calculator.model.vectors.DoubleVector;

import java.util.Objects;

public final class Constant implements Expression {
	private final double value;
	private DoubleVector vector;

	public Constant(double value) {
		this.value = value;
	}

	public double value() {
		return value;
	}

	public void set(DoubleVector vector) {
		this.vector = vector;
	}

	public DoubleVector evaluate() {
		return vector;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) return true;
		if (obj == null || obj.getClass() != this.getClass()) return false;
		var that = (Constant) obj;
		return Double.doubleToLongBits(this.value) == Double.doubleToLongBits(that.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(value);
	}

	@Override
	public String toString() {
		return "Const[" + value + ']';
	}

}
