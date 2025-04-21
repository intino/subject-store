package systems.intino.datamarts.subjectstore.io.statements;

import systems.intino.datamarts.subjectstore.io.Statements;
import systems.intino.datamarts.subjectstore.model.Statement;

import java.io.*;
import java.util.Iterator;
import java.util.stream.Stream;

public class DumpStatements implements Statements, Closeable {
	private final InputStream is;

	public DumpStatements(InputStream is) {
		this.is = is;
	}

	@Override
	public Iterator<Statement> iterator() {
		return lines().map(Statement::new).iterator();
	}

	private Stream<String> lines() {
		return new BufferedReader(new InputStreamReader(is)).lines();
	}

	public void close() throws IOException {
		is.close();
	}


}
