package systems.intino.datamarts.subjectstore.calculator.model.filters;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;

public record RollingAverageFilter(int windowSize) implements Filter {

	public RollingAverageFilter {
		if (windowSize <= 1) throw new IllegalArgumentException("Window size must be greater than 1");
	}

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];
		double sum = 0.0;

		for (int i = 0; i < input.length; i++) {
			sum += input[i];
			if (i >= windowSize) sum -= input[i - windowSize];
			output[i] = i >= windowSize - 1 ?
					sum / windowSize :
					Double.NaN;
		}
		return output;
	}
}