package systems.intino.datamarts.subjectstore.calculator.model.filters;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;

public record DifferencingFilter() implements Filter {

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];
		output[0] = Double.NaN;
		for (int i = 1; i < input.length; i++)
			output[i] = input[i] - input[i - 1];
		return output;
	}
}
