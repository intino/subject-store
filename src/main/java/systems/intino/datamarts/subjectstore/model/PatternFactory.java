package systems.intino.datamarts.subjectstore.model;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class PatternFactory {
	private static final Map<String, Pattern> patterns = new HashMap<>();

	public static Pattern pattern(String value) {
		return patterns.computeIfAbsent(value, s -> Pattern.compile(value));
	}


}
