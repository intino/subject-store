package systems.intino.datamarts.subjectstore.calculator.model.filters;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;

public record CosFilter() implements Filter {

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];
		for (int i = 0; i < input.length; i++)
			output[i] = Math.cos(input[i]);
		return output;
	}
}
