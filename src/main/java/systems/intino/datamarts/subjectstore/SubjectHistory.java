package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.io.Feeds;
import systems.intino.datamarts.subjectstore.io.feeds.RegistryFeeds;
import systems.intino.datamarts.subjectstore.io.HistoryRegistry.Row;
import systems.intino.datamarts.subjectstore.io.feeds.DumpFeeds;
import systems.intino.datamarts.subjectstore.io.HistoryRegistry;
import systems.intino.datamarts.subjectstore.io.registries.SqlHistoryRegistry;
import systems.intino.datamarts.subjectstore.model.*;
import systems.intino.datamarts.subjectstore.model.signals.CategoricalSignal;
import systems.intino.datamarts.subjectstore.model.signals.NumericalSignal;

import java.io.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static java.util.Comparator.comparingInt;
import static systems.intino.datamarts.subjectstore.TimeReferences.BigBang;
import static systems.intino.datamarts.subjectstore.TimeReferences.Legacy;
import static systems.intino.datamarts.subjectstore.TimeParser.parseInstant;

public class SubjectHistory implements AutoCloseable {
	private final String subject;
	private final HistoryRegistry registry;
	private final TagSet tagSet;
	private final Timeline timeline;

	public SubjectHistory(String subject, String storage) {
		this.subject = subject;
		this.registry = new SqlHistoryRegistry(subject, storage);
		this.tagSet = new TagSet(registry.tags());
		this.timeline = new Timeline(registry.instants());
	}

	public String subject() {
		return subject;
	}

	public String name() {
		String typedName = typedName();
		int i = typedName.lastIndexOf('.');
		return i >= 0 ? typedName.substring(0, i) : typedName;
	}

	public String type() {
		String typedName = typedName();
		int i = typedName.lastIndexOf('.');
		return i >= 0 ? typedName.substring(i + 1) : "";
	}

	public String typedName() {
		int i = subject.lastIndexOf('/');
		return subject.substring(i + 1).trim();
	}

	public int size() {
		return registry.size();
	}

	public Instant first() {
		return timeline.isEmpty() ? null : timeline.getFirst();
	}

	public Instant last() {
		return registry.isEmpty() ? null : timeline.getLast();
	}

	public boolean legacyExists() {
		return timeline.contains(Legacy);
	}

	public boolean bigbangExists() {
		return timeline.contains(BigBang);
	}

	public boolean legacyPending() {
		return legacyExists() && !bigbangExists();
	}

	public List<Instant> instants() {
		return timeline.instants();
	}

	public List<String> tags() {
		return tagSet.tags();
	}

	public boolean exists(String tag) {
		return tags().contains(tag);
	}

	public String ss(int feed) {
		return registry.ss(feed);
	}

	public Current current() {
		return new Current() {
			@Override
			public Number number(String tag) {
				return currentNumber(tag);
			}

			@Override
			public String text(String tag) {
				return currentText(tag);
			}
		};
	}

	public Query query() {
		return new Query() {
			@Override
			public NumericalQuery number(String tag) {
				return new NumericalQuery(tag);
			}

			@Override
			public CategoricalQuery text(String tag) {
				return new CategoricalQuery(tag);
			}
		};
	}

	private Number currentNumber(String tag) {
		Signal.Point<Number> current = query().number(tag).get();
		return current != null ? current.value() : null;
	}

	private String currentText(String tag) {
		Signal.Point<String> current = query().text(tag).get();
		return current != null ? current.value() : null;
	}

	private Signal.Point<Number> readNumber(String tag) {
		int feed = tagSet.lastUpdatingFeedOf(tag);
		if (feed == -1) return null;
		return new Signal.Point<>(
				feed,
				timeline.get(feed),
				registry.getNumber(tagSet.get(tag), feed)
		);
	}

	private List<Signal.Point<Double>> readNumbers(String tag, Instant from, Instant to) {
		return readNumbers(registry.getNumbers(tagSet.get(tag), timeline.from(from), timeline.to(to)));
	}

	private List<Signal.Point<Double>> readNumbers(Stream<Row> records) {
		return records.map(this::readNumber).toList();
	}

	private Signal.Point<Double> readNumber(Row row) {
		int feed = row.at(1).asInt();
		return new Signal.Point<>(feed, timeline.get(feed), row.at(2).asDouble());
	}

	private Signal.Point<String> readText(String tag) {
		int feed = tagSet.lastUpdatingFeedOf(tag);
		if (feed == -1) return null;
		return new Signal.Point<>(
				feed,
				timeline.get(feed),
				registry.getText(tagSet.get(tag), feed)
		);
	}

	private List<Signal.Point<String>> readTexts(String tag, Instant from, Instant to) {
		return readTexts(registry.getTexts(tagSet.get(tag), timeline.from(from), timeline.to(to)));
	}

	private List<Signal.Point<String>> readTexts(Stream<Row> records) {
		return records.map(this::readText).toList();
	}

	private Signal.Point<String> readText(Row row) {
		int feed = row.at(1).asInt();
		return new Signal.Point<>(feed, timeline.get(feed), row.at(2).asString());
	}

	public void dump(OutputStream os) throws IOException {
		for (Feed feed : feeds()) {
			feed.put("id", subject);
			String output = feed + "\n";
			os.write(output.getBytes());
		}
	}

	public Iterable<Feed> feeds() {
		Map<Integer, String> dictionary = tagSet.dictionary();
		return new RegistryFeeds(registry.dump(), dictionary::get);
	}

	public SubjectHistory restore(InputStream is) throws IOException {
		try (DumpFeeds feeds = new DumpFeeds(is)) {
			consume(feeds);
		}
		return this;
	}

	public void consume(Feeds feeds) {
		Batch batch = batch();
		for (Feed feed : feeds) {
			if (!feed.get("id").equals(subject)) continue;
			Transaction transaction = batch.on(feed.instant, feed.source);
			consume(feed, transaction);
			transaction.terminate();
		}
		batch.terminate();
	}

	private void consume(Feed feed, Transaction transaction) {
		for (String tag : feed.tags()) {
			if (tag.equals("id")) continue;
			Object value = feed.get(tag);
			if (value instanceof Double d)
				transaction.put(tag, d);
			else
				transaction.put(tag, value.toString());
		}
	}


	public void drop() {
		registry.drop();
	}

	@Override
	public void close()  {
		try {
			registry.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


	public class NumericalQuery {
		private final String tag;

		public NumericalQuery(String tag) {
			this.tag = tag;
		}

		public NumericalSignal all() {
			return get(first(), last());
		}

		public Signal.Point<Number> get() {
			return readNumber(tag);
		}

		public NumericalSignal get(String from, String to) {
			return get(parseInstant(from), parseInstant(to));
		}

		public NumericalSignal get(Instant from, Instant to) {
			return new NumericalSignal.Raw(from, to, readNumbers(tag, from, to));
		}

		public NumericalSignal get(TimeSpan span) {
			return get(span.from(), span.to());
		}

		@Override
		public String toString() {
			return "NumericalQuery(" + tag + ')';
		}
	}

	public class CategoricalQuery {

		private final String tag;

		public CategoricalQuery(String tag) {
			this.tag = tag;
		}

		public Signal.Point<String> get() {
			return readText(tag);
		}

		public CategoricalSignal all() {
			return get(first(), last());
		}

		public CategoricalSignal get(Instant from, Instant to) {
			return new CategoricalSignal.Raw(from, to, readTexts(tag, from, to));
		}

		public CategoricalSignal get(TimeSpan span) {
			return get(span.from(), span.to());
		}

		@Override
		public String toString() {
			return "CategoricalQuery(" + tag + ')';
		}
	}
	
	public Transaction on(String instant, String source) {
		return on(parseInstant(instant), source);
	}

	public Transaction on(Instant instant, String source) {
		return transaction(new Feed(instant, source));
	}

	private Transaction transaction(Feed feed) {
		return new Transaction() {
			@Override
			public Transaction put(String tag, String value) {
				feed.put(tag, value);
				return this;
			}

			@Override
			public Transaction put(String tag, Number value) {
				feed.put(tag, value);
				return this;
			}

			@Override
			public void terminate() {
				if (feed.isEmpty()) return;
				SubjectHistory.this.put(feed);
				registry.commit();
			}
		};
	}

	public Batch batch() {
		return new Batch() {
			private final List<Feed> feeds = new ArrayList<>();
			@Override
			public Transaction on(Instant instant, String source) {
				return new Transaction() {
					private final Feed feed = new Feed(instant, source);
					@Override
					public Transaction put(String tag, Number value) {
						feed.put(tag, value);
						return this;
					}

					@Override
					public Transaction put(String tag, String value) {
						feed.put(tag, value);
						return this;
					}

					@Override
					public void terminate() {
						if (feed.isEmpty()) return;
						feeds.add(feed);
					}
				};
			}

			@Override
			public void terminate() {
				feeds.forEach(feed -> put(feed));
				registry.commit();
			}
		};
	}

	private void put(Feed feed) {
		int id = registry.nextFeed();
		insertInstant(id, feed.instant);
		insertTags(feed.tags());
		insertFacts(id, feed);
	}

	private void insertInstant(int id, Instant instant) {
		timeline.add(instant, id);
	}

	private void insertTags(Set<String> tags) {
		for (String tag : tags) {
			int id = tagSet.add(tag);
			if (id < 0) continue;
			registry.setTag(id, tag);
		}
	}

	private void insertFacts(int id, Feed feed) {
		put("ts", feed.instant);
		put("ss", feed.source);
		for (String tag : feed.tags()) {
			put(tag, feed.get(tag));
			updateTag(tag, id, feed.instant);
		}
	}

	private void updateTag(String tag, int id, Instant instant) {
		if (instant.equals(Legacy)) return;
		if (lastUpdatingInstantOf(tag).isAfter(instant)) return;
		tagSet.update(tag, id);
		registry.setTagLastFeed(tagSet.get(tag), id);
	}

	private Instant lastUpdatingInstantOf(String tag) {
		return timeline.get(tagSet.lastUpdatingFeedOf(tag));
	}

	private void put(String tag, Object value) {
		registry.put(tagSet.get(tag), value);
	}

	public interface Transaction {
		Transaction put(String tag, Number value);
		Transaction put(String tag, String value);
		void terminate();
	}

	public interface Batch {
		Transaction on(Instant instant, String source);
		void terminate();
	}

	@Override
	public String toString() {
		return subject;
	}

	public static class RegistryException extends RuntimeException {
		public RegistryException(Exception exception) {
			super(exception);
		}
	}

	public static class TagSet {
		private final Map<String, Integer> labels;
		private final Map<Integer, Integer> lastUpdatingFeeds;

		TagSet(Stream<Row> rows)  {
			this.labels = new HashMap<>();
			this.lastUpdatingFeeds = new HashMap<>();
			this.init(rows);
		}

		int lastUpdatingFeedOf(String tag) {
			return contains(tag) ? lastUpdatingFeedOf(get(tag)) : -1;
		}

		int lastUpdatingFeedOf(int tag) {
			return lastUpdatingFeeds.getOrDefault(tag, -1);
		}

		int get(String tag) {
			return labels.get(tag);
		}

		List<String> tags() {
			return labels.entrySet().stream()
					.filter(e->e.getValue() > 1)
					.sorted(comparingInt(Map.Entry::getValue))
					.map(Map.Entry::getKey)
					.toList();
		}
		
		private void init(Stream<Row> records) {
			records.forEach(this::init);
		}

		private void init(Row row) {
			init(
					row.at(1).asInt(),
					row.at(2).asString(),
					row.at(3).asInt()
			);
		}

		private void init(int id, String label, int feed) {
			labels.put(label, id);
			lastUpdatingFeeds.put(id, feed);
		}

		private int add(String tag)  {
			if (contains(tag)) return -1;
			int index = labels.size();
			labels.put(tag, index);
			return index;
		}

		private void update(String tag, int feed) {
			lastUpdatingFeeds.put(get(tag), feed);
		}

		public boolean contains(String tag) {
			return labels.containsKey(tag);
		}

		private Map<Integer, String> dictionary() {
			Map<Integer, String> result = new HashMap<>();
			for (String tag : labels.keySet())
				result.put(get(tag), tag);
			return result;
		}
	}

	public static class Timeline {
		private final Map<Integer, Instant> instants;

		Timeline(Stream<Row> records) {
			this.instants = new HashMap<>();
			this.init(records);
		}

		Instant get(int feed) {
			if (feed < 0) return Instant.MIN;
			return instants.get(feed);
		}

		List<Instant> instants() {
			return new ArrayList<>(instants.values()).stream()
					.sorted()
					.toList();
		}

		public boolean contains(Instant instant) {
			return instants.values().stream().anyMatch(v->v.equals(instant));
		}

		public Instant getFirst() {
			return instants().getFirst();
		}

		public Instant getLast() {
			return instants().getLast();
		}

		public boolean isEmpty() {
			return instants.isEmpty();
		}

		void add(Instant instant, int feed) {
			instants.put(feed, instant);
		}

		private void init(Stream<Row> records) {
			records.forEach(this::init);
		}

		private void init(Row r) {
			instants.put(r.at(1).asInt(), r.at(2).asInstant());
		}

		private int from(Instant from) {
			if (from.equals(Instant.MIN)) return 0;
			int index = instants.size();
			Instant min = Instant.MAX;
			for (Map.Entry<Integer, Instant> entry : instants.entrySet()) {
				Instant instant = entry.getValue();
				if (instant.isBefore(from)) continue;
				if (instant.isAfter(min)) continue;
				index = entry.getKey();
				min = instant;
			}
			return index;
		}

		private int to(Instant to) {
			if (to.equals(Instant.MAX)) return instants.size();
			int index = -1;
			Instant max = Instant.MIN;
			for (Map.Entry<Integer, Instant> entry : instants.entrySet()) {
				Instant instant = entry.getValue();
				if (instant.isAfter(to)) continue;
				if (instant.isBefore(max)) continue;
				index = entry.getKey();
				max = instant;
			}
			return index;
		}
	}

	public interface Current {
		Number number(String tag);
		String text(String tag);
	}

	public interface Query {
		NumericalQuery number(String tag);
		CategoricalQuery text(String tag);
	}
}
