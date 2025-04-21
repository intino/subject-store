package systems.intino.datamarts.subjectstore.calculator.model.filters;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;

public record RollingMaxFilter(int windowSize) implements Filter {

	public RollingMaxFilter {
		if (windowSize <= 1) throw new IllegalArgumentException("Window size must be greater than 1");
	}

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];

		for (int i = 0; i < input.length; i++) {
			if (i < windowSize - 1) {
				output[i] = Double.NaN;
				continue;
			}

			double max = input[i - windowSize + 1];
			for (int j = i - windowSize + 1; j <= i; j++) {
				if (input[j] > max) max = input[j];
			}
			output[i] = max;
		}

		return output;
	}
}