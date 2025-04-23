package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.io.feeds.DumpFeeds;
import systems.intino.datamarts.subjectstore.model.Feed;
import systems.intino.datamarts.subjectstore.model.Subject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class SubjectStore implements AutoCloseable {
	private final SubjectIndex index;

	public SubjectStore(String jdbcUrl) {
		this.index = new SubjectIndex(jdbcUrl);
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

	public SubjectQuery subjects() {
		return index.subjects();
	}

	public void dumpIndex(OutputStream os) throws IOException {
		index.dump(os);
	}
	
	public void dumpHistories(OutputStream os) throws IOException {
		for (Subject subject : subjects().collect())
			if (subject.hasHistory())
				dump(subject.history(), os);
	}

	private void dump(SubjectHistory history, OutputStream os) throws IOException {
		history.dump(os);
	}

	public SubjectStore restoreIndex(InputStream is) throws IOException {
		index.restore(is);
		return this;
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

	private boolean isNewSubject(Feed feed, List<Feed> feeds) {
		return identifierIn(feed).compareTo(identifierIn(feeds)) != 0;
	}

	@SuppressWarnings("resource")
	private void consume(List<Feed> feeds) {
		if (feeds.isEmpty()) return;
		open(identifierIn(feeds)).history().consume(feeds);
		feeds.clear();
	}

	private String identifierIn(List<Feed> feeds) {
		return feeds.isEmpty() ? "" : identifierIn(feeds.getFirst());
	}

	private static String identifierIn(Feed first) {
		return (String) first.get("id");
	}

	@Override
	public void close()  {
		try {
			index.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


	public SubjectIndexView.Builder viewOf(List<Subject> subjects) {
		return SubjectIndexView.of(subjects);
	}

	public SubjectHistoryView.Builder viewOf(Subject subject) {
		return SubjectHistoryView.of(subject.history());
	}

	public SubjectHistoryView.Builder viewOf(String identifier) {
		return viewOf(open(identifier));
	}

	public SubjectHistoryView.Builder viewOf(String name, String type) {
		return viewOf(open(name, type));
	}


}
