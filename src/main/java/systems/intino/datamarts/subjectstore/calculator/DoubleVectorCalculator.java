package systems.intino.datamarts.subjectstore.calculator;

import systems.intino.datamarts.subjectstore.calculator.Expression.BinaryExpression;
import systems.intino.datamarts.subjectstore.calculator.Expression.UnaryExpression;
import systems.intino.datamarts.subjectstore.calculator.expressions.Constant;
import systems.intino.datamarts.subjectstore.calculator.expressions.NamedFunction;
import systems.intino.datamarts.subjectstore.calculator.expressions.Variable;
import systems.intino.datamarts.subjectstore.calculator.parser.Parser;
import systems.intino.datamarts.subjectstore.calculator.model.vectors.DoubleVector;

import java.util.Arrays;
import java.util.function.Function;

public class DoubleVectorCalculator {
	private final int size;
	private final Function<String, DoubleVector> variable;

	public DoubleVectorCalculator(int size, Function<String, DoubleVector> variable) {
		this.size = size;
		this.variable = variable;
	}

	public DoubleVector calculate(String formula) {
		Expression expression = fill(new Parser().parse(formula));
		double[] values = expression.evaluate().values();
		return new DoubleVector(values);
	}

	private Expression fill(Expression expression) {
		switch (expression) {
			case BinaryExpression e -> fill(e.left(), e.right());
			case UnaryExpression e -> fill(e.on());
			case NamedFunction e -> fill(e.on());
			case Constant e -> e.set(new DoubleVector(fill(e.value())));
			case Variable e -> e.set(get(e.name()));
			default -> {
			}
		}
		return expression;
	}

	private void fill(Expression... expressions) {
		for (Expression expression : expressions) fill(expression);
	}

	public DoubleVector get(String name) {
		return switch (name) {
			case "PI" -> new DoubleVector(fill(Math.PI));
			case "E" -> new DoubleVector(fill(Math.E));
			case "RANDOM" -> new DoubleVector(random());
			default -> variable.apply(name);
		};
	}

	private double[] random() {
		double[] values = new double[size];
		for (int i = 0; i < size; i++) values[i] = Math.random();
		return values;
	}

	private double[] fill(double value) {
		double[] values = new double[size];
		Arrays.fill(values, value);
		return values;
	}



}
