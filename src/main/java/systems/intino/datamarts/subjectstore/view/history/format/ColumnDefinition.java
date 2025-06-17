package systems.intino.datamarts.subjectstore.view.history.format;

import systems.intino.datamarts.subjectstore.calculator.model.Filter;

import java.util.ArrayList;
import java.util.List;

public class ColumnDefinition {
	public final String name;
	public final String expression;
	public final Type type;
	public final List<Filter> filters;

	public ColumnDefinition(String name, String expression, Type type) {
		this.name = name;
		this.expression = expression;
		this.type = type;
		this.filters = List.of();
	}

	public ColumnDefinition(String name, String expression, Filter... filters) {
		this.name = name;
		this.expression = expression;
		this.type = Type.Numerical;
		this.filters = list(filters);
	}

	public List<Filter> list(Filter... filters) {
		return new ArrayList<>(List.of(filters));
	}

	public ColumnDefinition add(Filter... filters) {
		this.filters.addAll(List.of(filters));
		return this;
	}

	public ColumnDefinition add(List<Filter> filters) {
		this.filters.addAll(filters);
		return this;
	}

	public enum Type {
		Numerical, Categorical
	}


}
