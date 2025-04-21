package systems.intino.datamarts.subjectstore.io;

import systems.intino.datamarts.subjectstore.model.Statement;

import java.util.function.Function;

public interface Statements extends Iterable<Statement> {

	default Schema schema() {
		return (key, mapper) -> null;
	}

	interface Schema {
		Schema map(String key, Function<String,String> mapper);
	}

}
