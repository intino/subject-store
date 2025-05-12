package systems.intino.datamarts.subjectstore.io.triples;

import systems.intino.datamarts.subjectstore.io.Triples;
import systems.intino.datamarts.subjectstore.model.Triple;

import java.io.*;
import java.util.Iterator;
import java.util.stream.Stream;

public class DumpTriples implements Triples, Closeable {
	private final InputStream is;

	public DumpTriples(InputStream is) {
		this.is = is;
	}

	@Override
	public Iterator<Triple> iterator() {
		return lines()
				.map(Triple::of)
				.iterator();
	}

	private Stream<String> lines() {
		return new BufferedReader(new InputStreamReader(is)).lines();
	}

	public void close() throws IOException {
		is.close();
	}


}
