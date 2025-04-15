package systems.intino.datamarts.subjectstore.calculator.model.filters;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;

import static java.lang.Double.isNaN;

public record BinaryThresholdFilter(double threshold) implements Filter {

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];
		for (int i = 0; i < input.length; i++)
			output[i] = isNaN(input[i]) ? valueOf(input[i]) : Double.NaN;
		return output;
	}

	private int valueOf(double value) {
		return value > threshold ? 1 : 0;
	}
}