package systems.intino.datamarts.subjectstore.history.view.fields;

import systems.intino.datamarts.subjectstore.model.signals.NumericalSignal;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.lang.Double.NaN;

public interface NumericalField extends Function<NumericalSignal, Double> {
	Map<String, NumericalField> map = create();

	private static Map<String, NumericalField> create() {
		Map<String, NumericalField> map = new HashMap<>();
		map.put("readings", s -> (double) s.count());
		map.put("sum", s -> s.summary().sum());
		map.put("average", s -> s.summary().mean());
		map.put("sd", s -> s.isEmpty() ? NaN : s.summary().sd());
		map.put("standard-deviation", s -> map.get("sd").apply(s));
		map.put("first", s -> s.isEmpty() ? NaN : s.summary().first().value());
		map.put("last", s -> s.isEmpty() ? NaN : s.summary().last().value());
		map.put("min", s -> s.isEmpty() ? NaN : s.summary().min().value());
		map.put("max", s -> s.isEmpty() ? NaN : s.summary().max().value());
		return map;
	}

	static boolean contains(String function) {
		return map.containsKey(function);
	}

	static NumericalField of(String name) {
		if (!map.containsKey(name)) throw new RuntimeException("Unknown function: " + name);
		return map.get(name);
	}



}
