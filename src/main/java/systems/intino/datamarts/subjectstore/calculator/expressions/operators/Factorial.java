package systems.intino.datamarts.subjectstore.calculator.expressions.operators;

import systems.intino.datamarts.subjectstore.calculator.Expression;
import systems.intino.datamarts.subjectstore.calculator.Expression.UnaryExpression;
import systems.intino.datamarts.subjectstore.calculator.model.vectors.DoubleVector;

public record Factorial(Expression on) implements UnaryExpression {

	public static Expression with(Expression operand) {
		return new Factorial(operand);
	}

	@Override
	public String toString() {
		return "Factorial[" + on + "]";
	}


	@Override
	public DoubleVector evaluate() {
		return on.evaluate().factorial();
	}
}
