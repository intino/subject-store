package io.intino.alexandria.model;

import java.time.Instant;

public record Point<T>(int feed, Instant instant, T value) {
	@Override
	public String toString() {
		return "[" + clean(instant) + "=" + value + ']';
	}

	private String clean(Instant instant) {
		return instant.toString().replaceAll("[-T:Z]","");
	}
}
