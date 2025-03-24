package io.intino.alexandria.model.table;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Format implements Iterable<Column> {
	private final List<Column> columns;
	private final Instant from;
	private final Instant to;
	private final Duration duration;

	public Format(Instant from, Instant to, Duration duration) {
		this.from = from;
		this.to = to;
		this.duration = duration;
		this.columns = new ArrayList<>();
	}

	public Instant from() {
		return from;
	}

	public Instant to() {
		return to;
	}

	public Duration duration() {
		return duration;
	}

	public List<Column> columns() {
		return columns;
	}

	public Format add(Column column) {
		this.columns.add(column);
		return this;
	}

	@Override
	public Iterator<Column> iterator() {
		return columns.iterator();
	}

	@Override
	public String toString() {
		return "Format(" + from + ", " + to + ")";
	}
}
