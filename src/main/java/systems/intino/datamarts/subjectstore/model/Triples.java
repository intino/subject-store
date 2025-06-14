package systems.intino.datamarts.subjectstore.model;

import java.util.function.Function;

public interface Triples extends Iterable<Triple> {

	default Schema schema() {
		return (key, mapper) -> null;
	}

	interface Schema {
		Schema map(String key, Function<String,String> mapper);
	}

}
