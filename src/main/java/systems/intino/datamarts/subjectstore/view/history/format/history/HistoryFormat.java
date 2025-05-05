package systems.intino.datamarts.subjectstore.view.history.format.history;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;

import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class HistoryFormat implements Iterable<ColumnDefinition> {
	private final List<ColumnDefinition> columnDefinitions;
	private final Instant from;
	private final Instant to;
	private final TemporalAmount duration;

	public HistoryFormat(Instant from, Instant to, TemporalAmount duration) {
		this(from, to, duration, new ArrayList<>());
	}

	public HistoryFormat(Instant from, Instant to, TemporalAmount duration, List<ColumnDefinition> columnDefinitions) {
		this.from = from;
		this.to = to;
		this.duration = duration;
		this.columnDefinitions = columnDefinitions;
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

	public List<ColumnDefinition> columns() {
		return columnDefinitions;
	}

	public HistoryFormat add(String name, String expression, Filter... filters) {
		ColumnDefinition columnDefinition = new ColumnDefinition(name, expression);
		Arrays.stream(filters).forEach(columnDefinition::add);
		this.columnDefinitions.add(columnDefinition);
		return this;
	}

	public HistoryFormat add(ColumnDefinition columnDefinition) {
		this.columnDefinitions.add(columnDefinition);
		return this;
	}

	public HistoryFormat add(List<ColumnDefinition> columnDefinitions) {
		this.columnDefinitions.addAll(columnDefinitions);
		return this;
	}

	@Override
	public Iterator<ColumnDefinition> iterator() {
		return columnDefinitions.iterator();
	}

	@Override
	public String toString() {
		return "Format(" + from + ", " + to + ")";
	}
}
