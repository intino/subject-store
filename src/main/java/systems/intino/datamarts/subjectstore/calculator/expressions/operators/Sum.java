package systems.intino.datamarts.subjectstore.calculator.expressions.operators;

import systems.intino.datamarts.subjectstore.calculator.Expression;
import systems.intino.datamarts.subjectstore.calculator.model.vectors.DoubleVector;

public record Sum(Expression left, Expression right) implements Expression.BinaryExpression {

	public static Expression with(Expression right, Expression left) {
		return new Sum(left, right);
	}

	@Override
	public String toString() {
		return "Sum[" + left + ", " + right + "]";
	}

	@Override
	public DoubleVector evaluate() {
		return left.evaluate().add(right.evaluate());
	}
}
