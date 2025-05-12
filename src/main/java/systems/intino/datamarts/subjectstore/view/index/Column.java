package systems.intino.datamarts.subjectstore.view.index;

import systems.intino.datamarts.subjectstore.view.Stats;

public interface Column {
	String name();
	Type type();

	Stats stats();

	enum Type {
		Text, Number, Instant
	}

}
