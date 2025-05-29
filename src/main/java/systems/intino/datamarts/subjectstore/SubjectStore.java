package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.io.triples.DumpTriples;
import systems.intino.datamarts.subjectstore.model.Journal;
import systems.intino.datamarts.subjectstore.model.Subject;
import systems.intino.datamarts.subjectstore.model.journals.FileJournal;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;

public class SubjectStore {
	private final File indexFile;
	private final SubjectIndex index;
	private Connection connection;

	public SubjectStore(File indexFile) throws IOException {
		this.indexFile = indexFile;
		this.index = initIndex();
		this.connection = null;
	}

	public SubjectStore connection(Connection connection) {
		this.connection = connection;
		tryDisableAutoCommit();
		return this;
	}

	public boolean has(String identifier) {
		return index.has(identifier);
	}

	public boolean has(String name, String type) {
		return index.has(name, type);
	}

	public Subject open(String identifier) {
		return index.open(identifier);
	}

	public Subject open(String name, String type) {
		return index.open(name, type);
	}

	public Subject create(String identifier) {
		return index.create(identifier);
	}

	public Subject create(String name, String type) {
		return index.create(name, type);
	}

	public SubjectHistory historyOf(String identifier) {
		return historyOf(new Subject(identifier, index.context()));
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	public void seal() throws IOException {
		try (OutputStream os = new BufferedOutputStream(new FileOutputStream(indexFile))) {
			index.dump(os);
			journalFile().delete();
		}
	}

	public SubjectIndex restore(Journal journal) {
		return index.restore(journal);
	}

	public SubjectIndex.Batch batch() {
		return index.batch();
	}

	public SubjectQuery subjects() {
		return index.subjects();
	}

	public SubjectQuery subjects(String query) {
		return index.subjects(query);
	}

	public SubjectHistory historyOf(Subject subject) {
		if (connection == null) throw new IllegalStateException("Historical database is not configured. Define store.connection(...) before using historyOf().");
		return new SubjectHistory(subject.identifier(), connection);
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	private SubjectIndex initIndex() throws IOException {
		File journalFile = journalFile();
		File recoverFile = new File(journalFile.getAbsolutePath() + ".recovering");
		journalFile.renameTo(recoverFile);
		try (InputStream is = inputStream()) {
			SubjectIndex index = new SubjectIndex(journalFile).restore(new DumpTriples(is)).restore(new FileJournal(recoverFile));
			recoverFile.delete();
			return index;
		}
	}

	private InputStream inputStream() throws FileNotFoundException {
		return indexFile.exists() ? new BufferedInputStream(new FileInputStream(indexFile)) : new ByteArrayInputStream(new byte[0]);
	}

	private File journalFile() {
		return new File(indexFile.getAbsolutePath() + ".journal");
	}

	private void tryDisableAutoCommit() {
		if (connection == null) return;
		try {
			connection.setAutoCommit(false);
		} catch (SQLException ignored) {
		}
	}

}
