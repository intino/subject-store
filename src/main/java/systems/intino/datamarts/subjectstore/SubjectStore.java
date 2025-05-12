package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.io.feeds.DumpFeeds;
import systems.intino.datamarts.subjectstore.io.triples.DumpTriples;
import systems.intino.datamarts.subjectstore.model.Feed;
import systems.intino.datamarts.subjectstore.model.Subject;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

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
		try (InputStream is = new BufferedInputStream(new FileInputStream(indexFile))) {
			return new SubjectIndex(journalFile).restore(new DumpTriples(is)).restore(new Journal(recoverFile));
		}
		finally {
			recoverFile.delete();
		}
	}

	private File journalFile() {
		return new File(indexFile.getAbsolutePath() + ".journal");
	}

	private static String clean(String ts) {
		return ts.replace("-", "")
				.replace(":", "")
				.replace("T", "")
				.substring(0, 14);
	}

	private void dump(SubjectHistory history, OutputStream os) throws IOException {
		history.dump(os);
	}
	
	public SubjectStore restoreHistories(InputStream is) throws IOException {
		DumpFeeds dump = new DumpFeeds(is);
		List<Feed> feeds = new ArrayList<>();
		for (Feed feed : dump) {
			if (isNewSubject(feed, feeds)) consume(feeds);
			feeds.add(feed);
		}
		consume(feeds);
		return this;
	}

	public void dumpHistories(OutputStream os) throws IOException {
		for (Subject subject : subjects().collect()) {

		}
	}

	private boolean isNewSubject(Feed feed, List<Feed> feeds) {
		return identifierIn(feed).compareTo(identifierIn(feeds)) != 0;
	}

	private void consume(List<Feed> feeds) {
		if (feeds.isEmpty()) return;
		//TODO open(identifierIn(feeds)).history().consume(feeds);
		feeds.clear();
	}

	private String identifierIn(List<Feed> feeds) {
		return feeds.isEmpty() ? "" : identifierIn(feeds.getFirst());
	}

	private static String identifierIn(Feed first) {
		return (String) first.get("id");
	}

	public SubjectIndexView.Builder viewOf(List<Subject> subjects) {
		return SubjectIndexView.of(subjects);
	}

	public SubjectHistoryView.Builder viewOf(Subject subject) {
		return SubjectHistoryView.of(null);
	}

	public SubjectHistoryView.Builder viewOf(String identifier) {
		return viewOf(open(identifier));
	}

	public SubjectHistoryView.Builder viewOf(String name, String type) {
		return viewOf(open(name, type));
	}


	@Override
	public void close() {

	}
}
