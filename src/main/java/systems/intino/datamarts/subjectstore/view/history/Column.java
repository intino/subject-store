package systems.intino.datamarts.subjectstore.view.history;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Column {
	public final String name;
	public final String definition;
	public final List<Filter> filters;

	public Column(String name, String definition) {
		this.name = name;
		this.definition = definition;
		this.filters = new ArrayList<>();
	}

	public Column add(Filter filter) {
		filters.add(filter);
		return this;
	}

	public Column add(List<Filter> filters) {
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
				.anyMatch(definition::contains);
	}

}
