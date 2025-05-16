package systems.intino.datamarts.subjectstore.view.history.format;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;

import java.util.ArrayList;
import java.util.List;

public class ColumnDefinition {
	public final String name;
	public final String expression;
	public final List<Filter> filters;

	public ColumnDefinition(String name, String expression, Filter... filters) {
		this.name = name;
		this.expression = expression;
		this.filters = new ArrayList<>();
		this.add(filters);
	}

	public ColumnDefinition add(Filter... filters) {
		this.filters.addAll(List.of(filters));
		return this;
	}

	public ColumnDefinition add(List<Filter> filters) {
		this.filters.addAll(filters);
		return this;
	}


}
