package systems.intino.datamarts.subjectstore.view.index;

public record Column(String name, String[] values, Summary summary) {
	public Column(String name, String[] values) {
		this(name, values, Summary.of(values));
	}

	@Override
	public String toString() {
		return name;
	}
}
