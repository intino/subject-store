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
		map.put("first", s -> hasCategoricalContent(s) ? categorical(s).points().getFirst().value() : "");
		map.put("last", s -> hasCategoricalContent(s) ? categorical(s).points().getLast().value() : "");
		map.put("mode", s -> hasCategoricalContent(s) ? categorical(s).summary().mode() : "");
		return map;
	}

	static boolean contains(String field) {
		return map.containsKey(field);
	}

	static TextReducer of(String field) {
		return map.containsKey(field) ? map.get(field) : s-> "Unknown function: " + field;
	}

}
