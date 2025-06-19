package systems.intino.datamarts.subjectstore.model.reducers;

import systems.intino.datamarts.subjectstore.model.Signal;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.lang.Double.NaN;
import static systems.intino.datamarts.subjectstore.model.signals.CategoricalSignal.*;
import static systems.intino.datamarts.subjectstore.model.signals.NumericalSignal.*;

public interface NumberReducer extends Function<Signal<?>, Double> {
	Map<String, NumberReducer> map = create();

	private static Map<String, NumberReducer> create() {
		Map<String, NumberReducer> map = new HashMap<>();
		map.put("count", s -> (double) s.count());
		map.put("sum", s -> isNumerical(s) ? numerical(s).summary().sum() : NaN);
		map.put("total", map.get("sum"));
		map.put("mean", s -> isNumerical(s) ? numerical(s).summary().mean() : NaN);
		map.put("average", map.get("mean"));
		map.put("sd", s -> hasNumericalContent(s) ? numerical(s).summary().sd() : NaN);
		map.put("first", s -> hasNumericalContent(s) ? numerical(s).summary().first().value() : NaN);
		map.put("last", s -> hasNumericalContent(s) ? numerical(s).summary().last().value() : NaN);
		map.put("min", s -> hasNumericalContent(s) ? numerical(s).summary().min().value() : NaN);
		map.put("max", s -> hasNumericalContent(s) ? numerical(s).summary().max().value() : NaN);
		map.put("entropy", s -> hasCategoricalContent(s) ? categorical(s).summary().entropy() : NaN);
		return map;
	}


	static boolean contains(String field) {
		return map.containsKey(field);
	}

	static NumberReducer of(String name) {
		if (!map.containsKey(name)) throw new RuntimeException("Unknown function: " + name);
		return map.get(name);
	}



}
