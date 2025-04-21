package systems.intino.datamarts.subjectstore.model.reducers;

import systems.intino.datamarts.subjectstore.model.Signal;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static systems.intino.datamarts.subjectstore.model.signals.CategoricalSignal.categorical;
import static systems.intino.datamarts.subjectstore.model.signals.CategoricalSignal.hasCategoricalContent;

public interface TextReducer extends Function<Signal<?>, String> {
	Map<String, TextReducer> map = create();

	private static Map<String, TextReducer> create() {
		Map<String, TextReducer> map = new HashMap<>();
		map.put("mode", s -> hasCategoricalContent(s) ? categorical(s).summary().mode() : "");
		return map;
	}

	static boolean contains(String function) {
		return map.containsKey(function);
	}

	static TextReducer of(String name) {
		return map.containsKey(name) ? map.get(name) : s-> "Unknown function: " + name;
	}

}
