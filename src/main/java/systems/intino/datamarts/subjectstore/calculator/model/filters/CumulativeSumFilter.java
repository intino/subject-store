package systems.intino.datamarts.subjectstore.calculator.model.filters;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;

public record CumulativeSumFilter() implements Filter {

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];
		double sum = 0.0;

		for (int i = 0; i < input.length; i++) {
			sum += input[i];
			output[i] = sum;
		}

		return output;
	}
}