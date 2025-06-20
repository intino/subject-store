package systems.intino.datamarts.subjectstore.view.history.format;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;
import systems.intino.datamarts.subjectstore.view.history.format.ColumnDefinition.Type;

import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;

public class HistoryFormat {
	private final RowDefinition rowDefinition;
	private final List<ColumnDefinition> columnDefinitions;

	public HistoryFormat(RowDefinition rowDefinition) {
		this(rowDefinition, new ArrayList<>());
	}

	public HistoryFormat(RowDefinition rowDefinition, List<ColumnDefinition> columnDefinitions) {
		this.rowDefinition = rowDefinition;
		this.columnDefinitions = columnDefinitions;
	}

	public Instant from() {
		return rowDefinition.from;
	}

	public Instant to() {
		return rowDefinition.to;
	}

	public TemporalAmount period() {
		return rowDefinition.period;
	}

	public List<ColumnDefinition> columns() {
		return columnDefinitions;
	}

	public HistoryFormat add(String name, String expression, Type type) {
		return add(new ColumnDefinition(name, expression, type));
	}

	public HistoryFormat add(String name, String expression, Filter... filters) {
		return add(new ColumnDefinition(name, expression, filters));
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
	public String toString() {
		return "Format(" + rowDefinition + ")";
	}

	public record RowDefinition(Instant from, Instant to, TemporalAmount period) {
		@Override
		public String toString() {
			return from + "," + to;
		}
	}
}
