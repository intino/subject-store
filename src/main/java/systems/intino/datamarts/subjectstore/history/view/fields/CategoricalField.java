package systems.intino.datamarts.subjectstore.history.view.fields;

import systems.intino.datamarts.subjectstore.history.model.Sequence;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.lang.Double.NaN;

public interface CategoricalField extends Function<Sequence, Object> {
	Map<String, CategoricalField> map = create();

	private static Map<String, CategoricalField> create() {
		Map<String, CategoricalField> map = new HashMap<>();
		map.put("count", s -> (double) s.count());
		map.put("entropy", s -> s.isEmpty() ? NaN : s.summary().entropy());
		map.put("mode", s -> s.isEmpty() ? "" : s.summary().mode());
		return map;
	}

	static boolean contains(String function) {
		return map.containsKey(function);
	}

	static CategoricalField of(String name) {
		return map.containsKey(name) ? map.get(name) : s-> "Unknown function: " + name;
	}

}
