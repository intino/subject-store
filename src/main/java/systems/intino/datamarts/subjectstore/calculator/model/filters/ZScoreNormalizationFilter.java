package systems.intino.datamarts.subjectstore.calculator.model.filters;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;

public record ZScoreNormalizationFilter() implements Filter {

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];
		double mean = mean(input);
		double std = stddev(input, mean);

		for (int i = 0; i < input.length; i++)
			output[i] = std > 0 ? (input[i] - mean) / std : Double.NaN;
		return output;
	}

	private double mean(double[] data) {
		double sum = 0.0;
		for (double x : data) sum += x;
		return sum / data.length;
	}

	private double stddev(double[] data, double mean) {
		double sum = 0.0;
		for (double x : data) sum += Math.pow(x - mean, 2);
		return Math.sqrt(sum / data.length);
	}
}
