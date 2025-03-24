package io.intino.alexandria.io;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.intino.alexandria.model.Instants.Legacy;

public final class Transaction {
	public final Instant instant;
	public final String source;
	public final Map<String, Object> facts;

	public Transaction(Instant instant, String source) {
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

	public boolean isLegacy() {
		return instant.equals(Legacy);
	}
}
