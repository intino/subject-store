package systems.intino.datamarts.subjectstore.calculator.model.filters;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;

public record LeadFilter(int lag) implements Filter {

	public LeadFilter {
		if (lag < 0) throw new IllegalArgumentException("Lead must be non-negative");
	}

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];

		for (int i = 0; i < input.length; i++)
			output[i] = (i + lag < input.length) ? input[i + lag] : Double.NaN;

		return output;
	}
}
