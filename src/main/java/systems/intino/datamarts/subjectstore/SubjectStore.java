package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.io.triples.DumpTriples;
import systems.intino.datamarts.subjectstore.model.Subject;

import java.io.*;

public class SubjectStore implements AutoCloseable {
	private final File indexFile;
	private final SubjectIndex index;
	private final String historyJdbcUrl;

	public SubjectStore(File indexFile) throws IOException {
		this.indexFile = indexFile;
		this.index = createIndex();
		this.historyJdbcUrl = null;
	}

	public SubjectStore(File indexFile, String historyJdbcUrl) throws IOException {
		this.indexFile = indexFile;
		this.index = createIndex();
		this.historyJdbcUrl = historyJdbcUrl;
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	public void seal() throws IOException {
		try (OutputStream os = new BufferedOutputStream(new FileOutputStream(indexFile))) {
			index.dump(os);
			journalFile().delete();
		}
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

	public SubjectHistory historyOf(String subject) {
		return historyOf(new Subject(subject));
	}

	public SubjectHistory historyOf(Subject subject) {
		return new SubjectHistory(subject.identifier(), historyJdbcUrl);
	}

	public SubjectQuery subjects() {
		return index.subjects();
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	private SubjectIndex createIndex() throws IOException {
		File journalFile = journalFile();
		File recoverFile = new File(journalFile.getAbsolutePath() + ".recovering");
		journalFile.renameTo(recoverFile);
		try (InputStream is = inputStream()) {
			SubjectIndex index = new SubjectIndex(journalFile).restore(new DumpTriples(is)).restore(new SubjectIndex.Journal(recoverFile));
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

	@Override
	public void close() {

	}
}
