package systems.intino.datamarts.subjectstore.calculator.expressions.operators;

import systems.intino.datamarts.subjectstore.calculator.Expression;
import systems.intino.datamarts.subjectstore.calculator.Expression.BinaryExpression;
import systems.intino.datamarts.subjectstore.calculator.model.vectors.DoubleVector;

public record Division(Expression left, Expression right) implements BinaryExpression {

	public static Expression with(Expression right, Expression left) {
		return new Division(left, right);
	}

	@Override
	public String toString() {
		return "Div[" + left + ", " + right + "]";
	}


	@Override
	public DoubleVector evaluate() {
		return left.evaluate().div(right.evaluate());
	}
}
