package systems.intino.datamarts.subjectstore.index.view;

public record Column(String name, String[] values, Summary summary) {
	public Column(String name, String[] values) {
		this(name, values, Summary.of(values));
	}

	@Override
	public String toString() {
		return name;
	}
}
