package systems.intino.datamarts.subjectstore.calculator.model.vectors;

import systems.intino.datamarts.subjectstore.calculator.model.Vector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public record DoubleVector(double[] values) implements Vector<Double> {

	public DoubleVector add(DoubleVector vector) {
		double[] result = new double[values.length];
		for (int i = 0; i < values.length; i++)
			result[i] = values[i] + vector.values[i];
		return new DoubleVector(result);
	}

	public DoubleVector sub(DoubleVector vector) {
		double[] result = new double[values.length];
		for (int i = 0; i < values.length; i++)
			result[i] = values[i] - vector.values[i];
		return new DoubleVector(result);
	}

	public DoubleVector mul(DoubleVector vector) {
		double[] result = new double[values.length];
		for (int i = 0; i < values.length; i++)
			result[i] = values[i] * vector.values[i];
		return new DoubleVector(result);
	}

	public DoubleVector div(DoubleVector vector) {
		double[] result = new double[values.length];
		for (int i = 0; i < values.length; i++)
			result[i] = values[i] / vector.values[i];
		return new DoubleVector(result);
	}

	public DoubleVector module(DoubleVector vector) {
		double[] result = new double[values.length];
		for (int i = 0; i < values.length; i++)
			result[i] = values[i] % vector.values[i];
		return new DoubleVector(result);
	}

	public DoubleVector factorial() {
		double[] result = new double[values.length];
		for (int i = 0; i < values.length; i++)
			result[i] = factorial((int) values[i]);
		return new DoubleVector(result);
	}
	private static final Map<Long, Long> memoize = new HashMap<>();

	private long factorial(long value) {
		return value > 1 ? memoize.computeIfAbsent(value, x -> x * factorial(x - 1)) : 1;
	}

	@Override
	public Double get(int index) {
		return values[index];
	}

	@Override
	public int size() {
		return values.length;
	}

	@Override
	public String toString() {
		return Arrays.toString(values);
	}
}
