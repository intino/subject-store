package systems.intino.datamarts.subjectstore.calculator.model.filters;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;

public record LagFilter(int lag) implements Filter {

	public LagFilter {
		if (lag < 0) throw new IllegalArgumentException("Lag must be non-negative");
	}

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];

		for (int i = 0; i < input.length; i++) {
			output[i] = (i < lag) ? Double.NaN : input[i - lag];
		}

		return output;
	}
}
