package systems.intino.datamarts.subjectstore.view.history.format;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ColumnDefinition {
	public final String name;
	public final String expression;
	public final List<Filter> filters;

	public ColumnDefinition(String name, String expression) {
		this.name = name;
		this.expression = expression;
		this.filters = new ArrayList<>();
	}

	public ColumnDefinition add(Filter filter) {
		filters.add(filter);
		return this;
	}

	public ColumnDefinition add(List<Filter> filters) {
		this.filters.addAll(filters);
		return this;
	}

	private static final Set<String> AlphanumericRules = Set.of(
			"ts.year-quarter",
			"ts.year-month",
			"ts.year-month-day",
			"ts.year-month-day-hour",
			"ts.year-month-day-hour-minute",
			"ts.year-month-day-hour-minute-second",
			".mode"
	);

	public boolean isAlphanumeric() {
		return AlphanumericRules.stream()
				.anyMatch(expression::contains);
	}

}
