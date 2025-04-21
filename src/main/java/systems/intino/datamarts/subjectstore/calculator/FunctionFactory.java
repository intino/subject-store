package systems.intino.datamarts.subjectstore.calculator;

import systems.intino.datamarts.subjectstore.calculator.model.vectors.DoubleVector;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class FunctionFactory {
	private static final Map<String, Function<Double,Double>> Functions = new HashMap<>();
	public static Function<DoubleVector, DoubleVector> create(String name) {
		return vectorize(Functions.getOrDefault(name.toLowerCase(), v -> notFound(name)));
	}

	private static Double notFound(String name) {
		throw new IllegalArgumentException("Function not found: " + name);
	}

	private static Function<DoubleVector, DoubleVector> vectorize(Function<Double, Double> function) {
		return vector -> {
			double[] values = vector.values().clone();
			for (int i = 0; i < values.length; i++)
				values[i] = function.apply(values[i]);
			return new DoubleVector(values);
		};
	}

	static {
		Functions.put("abs", Math::abs);
		Functions.put("negate", v -> -v);
		Functions.put("round", v -> (double) Math.round(v));
		Functions.put("floor", Math::floor);
		Functions.put("ceil", Math::ceil);
		Functions.put("signum", Math::signum);

		Functions.put("exp", Math::exp);
		Functions.put("log", Math::log);
		Functions.put("log10", Math::log10);

		Functions.put("sqr", v-> v * v);
		Functions.put("sqrt", Math::sqrt);
		Functions.put("cbrt", Math::cbrt);

		Functions.put("rad", Math::toRadians);
		Functions.put("deg", Math::toDegrees);

		Functions.put("sin", Math::sin);
		Functions.put("cos", Math::cos);
		Functions.put("tan", Math::tan);

		Functions.put("asin", Math::asin);
		Functions.put("acos", Math::acos);
		Functions.put("atan", Math::atan);

		Functions.put("sinh", Math::sinh);
		Functions.put("cosh", Math::cosh);
		Functions.put("tanh", Math::tanh);
	}

}
