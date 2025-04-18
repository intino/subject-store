package systems.intino.datamarts.subjectstore.view.history;

import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Format implements Iterable<Column> {
	private final List<Column> columns;
	private final Instant from;
	private final Instant to;
	private final TemporalAmount duration;

	public Format(Instant from, Instant to, TemporalAmount duration) {
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

	public TemporalAmount duration() {
		return duration;
	}

	public List<Column> columns() {
		return columns;
	}

	public Format add(Column column) {
		this.columns.add(column);
		return this;
	}

	public Format add(List<Column> columns) {
		this.columns.addAll(columns);
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
