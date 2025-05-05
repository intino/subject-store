package systems.intino.datamarts.subjectstore.view.index;

public interface Column {
	String name();
	Type type();
	enum Type {
		Text, Number, Instant
	}

}
