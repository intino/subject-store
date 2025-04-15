package systems.intino.datamarts.subjectstore.history.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class Feed {
	public final Instant instant;
	public final String source;
	public final Map<String, Object> facts;

	public Feed(Instant instant, String source) {
		this.instant = instant;
		this.source = source;
		this.facts = new HashMap<>();
	}

	public void put(String name, Object value) {
		facts.put(name, value);
	}

	public Set<String> tags() {
		return facts.keySet();
	}

	public Object get(String tag) {
		return facts.get(tag);
	}

	public boolean isEmpty() {
		return facts.isEmpty();
	}

	@Override
	public String toString() {
		return String.join("\n",
				"[subject]",
				"ts=" + instant.toString(),
				"ss=" + source,
				toString(facts.entrySet())
		);
	}

	private String toString(Set<Map.Entry<String, Object>> entries) {
		return entries.stream().map(Feed::toString).collect(Collectors.joining("\n"));
	}

	private static String toString(Map.Entry<String, Object> e) {
		return e.getKey() + "=" + e.getValue();
	}

	public Iterator<String> iterator() {
		return facts.keySet().iterator();
	}
}
