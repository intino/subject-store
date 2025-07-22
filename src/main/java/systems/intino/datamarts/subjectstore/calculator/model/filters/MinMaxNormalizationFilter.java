package systems.intino.datamarts.subjectstore.calculator.model.filters;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;

import static java.lang.Double.NaN;

public record MinMaxNormalizationFilter() implements Filter {

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];

		double min = Double.POSITIVE_INFINITY;
		double max = Double.NEGATIVE_INFINITY;

		for (double value : input) {
			if (value < min) min = value;
			if (value > max) max = value;
		}

		double range = max - min;
		for (int i = 0; i < input.length; i++)
			output[i] = range != 0 ? (input[i] - min) / range : 0;

		return output;
	}
}