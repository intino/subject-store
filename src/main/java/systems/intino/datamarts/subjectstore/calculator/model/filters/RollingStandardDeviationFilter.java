package systems.intino.datamarts.subjectstore.calculator.model.filters;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;

public record RollingStandardDeviationFilter(int windowSize) implements Filter {

	public RollingStandardDeviationFilter {
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

			double sum = 0.0;
			double sumSquare = 0.0;

			for (int j = i - windowSize + 1; j <= i; j++) {
				sum += input[j];
				sumSquare += input[j] * input[j];
			}

			double mean = sum / windowSize;
			double variance = (sumSquare / windowSize) - (mean * mean);
			output[i] = Math.sqrt(variance);
		}

		return output;
	}
}