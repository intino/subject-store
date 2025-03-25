package io.intino.alexandria.io.registries;

import io.intino.alexandria.io.Registry;
import io.intino.alexandria.io.Transaction;
import io.intino.alexandria.model.Point;
import io.intino.alexandria.SubjectStore.RegistryException;

import java.io.File;
import java.sql.*;
import java.time.Instant;
import java.util.*;

import static io.intino.alexandria.model.Instants.BigBang;
import static io.intino.alexandria.model.Instants.Legacy;
import static java.sql.Types.*;
import static java.util.Comparator.comparingInt;

public class SqliteRegistry implements Registry {
	private final String name;
	private final Connection connection;
	private final StatementProvider statementProvider;
	private final TagSet tagSet;
	private final Timeline timeline;
	private int feeds;

	static { loadDriver(); }

	public SqliteRegistry(File file) {
		try {
			this.name = withoutExtension(file.getName());
			this.connection = isCreated(file) ? ConnectionProvider.open(file) : ConnectionProvider.create(file);
			this.statementProvider = new StatementProvider();
			this.feeds = readFeedCount();
			this.tagSet = new TagSet();
			this.timeline = new Timeline();
		} catch (SQLException e) {
			throw new RegistryException(e);
		}
	}

	private String withoutExtension(String name) {
		return name.substring(0, name.lastIndexOf('.'));
	}

	private static boolean isCreated(File file) {
		return file.exists() && file.length() > 0;
	}

	public String name() {
		return name;
	}

	@Override
	public int feeds() {
		return feeds;
	}

	public List<String> tags() {
		return tagSet.tags();
	}

	public List<Instant> instants() {
		return timeline.instants();
	}

	@Override
	public String ss(int feed) {
		try {
			return readText(1, feed);
		} catch (SQLException e) {
			throw new RegistryException(e);
		}
	}

	@Override
	public Point<Long> readLong(String tag) {
		try {
			return tagSet.contains(tag) ? readLong(tagSet.get(tag)) : null;
		} catch (SQLException e) {
			throw new RegistryException(e);
		}
	}

	@Override
	public List<Point<Long>> readLongs(String tag, Instant from, Instant to) {
		try {
			return tagSet.contains(tag) ? readLongs(tagSet.get(tag), timeline.from(from), timeline.to(to)) : List.of();
		} catch (SQLException e) {
			throw new RegistryException(e);
		}
	}

	@Override
	public Point<String> readText(String tag) {
		try {
			return tagSet.contains(tag) ? readText(tagSet.get(tag)) : null;
		} catch (SQLException e) {
			throw new RegistryException(e);
		}
	}

	@Override
	public List<Point<String>> readTexts(String tag, Instant from, Instant to) {
		try {
			return tagSet.contains(tag) ? readTexts(tagSet.get(tag), timeline.from(from), timeline.to(to)) : List.of();
		} catch (SQLException e) {
			throw new RegistryException(e);
		}
	}

	private List<Point<Long>> readLongs(int tag, int from, int to) throws SQLException {
		List<Point<Long>> timeEntries = new ArrayList<>();
		try (ResultSet rs = selectLongValues(tag, from, to)) {
			while (rs.next()) {
				int feed = rs.getInt(1);
				timeEntries.add(new Point<>(feed, timeline.get(feed), rs.getLong(2)));
			}
		}
		return timeEntries;
	}

	private Point<Long> readLong(int tag) throws SQLException {
		int feed = tagSet.lastUpdatingFeedOf(tag);
		return feed < 0 ? null : new Point<>(
				feed,
				timeline.get(feed),
				readLong(tag, feed)
		);
	}

	private Point<String> readText(int tag) throws SQLException {
		int feed = tagSet.lastUpdatingFeedOf(tag);
		return feed < 0 ? null : new Point<>(
				feed,
				timeline.get(feed),
				readText(tag, feed)
		);
	}

	@Override
	public void register(Transaction transaction) {
		try {
			updateInstants(transaction);
			updateTags(transaction);
			store(transaction);
			updateFeed();
			connection.commit();
		} catch (SQLException e) {
			throw new RegistryException(e);
		}
	}

	private List<Point<String>> readTexts(int tag, int from, int to) throws SQLException {
		List<Point<String>> timeEntries = new ArrayList<>();
		try (ResultSet rs = selectTextValues(tag, from, to)) {
			while (rs.next()) {
				int feed = rs.getInt(1);
				timeEntries.add(new Point<>(feed, timeline.get(feed), rs.getString(2)));
			}
		}
		return timeEntries;
	}

	private void store(Transaction transaction) throws SQLException {
		insertEntry("ts", transaction.instant);
		insertEntry("ss", transaction.source);
		for (String tag : transaction.tags())
			insertEntry(tag, transaction.get(tag));
	}

	private void updateInstants(Transaction transaction) {
		timeline.update(transaction.instant);
	}

	private void updateTags(Transaction transaction) throws SQLException {
		tagSet.insert(transaction.tags());
		updateTagFeed(tagSet.get("ts"));
		updateTagFeed(tagSet.get("ss"));
		if (transaction.isLegacy()) return;
		tagSet.update(transaction.tags(), transaction.instant);
	}

	private static final int FEEDS = -1;

	private int readFeedCount() throws SQLException {
		return (int) readLong(FEEDS, FEEDS);
	}

	private long readLong(int tag, int feed) throws SQLException {
		try (ResultSet select = selectLongValue(tag, feed)) {
			return select.getLong(1);
		}
	}

	private String readText(int tag, int feed) throws SQLException {
		try (ResultSet select = selectTextValue(tag, feed)) {
			return select.getString(1);
		}
	}

	private void insertEntry(String tag, Object o) throws SQLException {
		switch (type(o)) {
			case BIGINT -> insertEntry(tagSet.get(tag), ((Number) o).longValue());
			case DATE -> insertEntry(tagSet.get(tag), ((Instant) o));
			case VARCHAR -> insertEntry(tagSet.get(tag), o.toString());
		}
	}

	private int type(Object o) {
		if (o instanceof Number) return BIGINT;
		if (o instanceof Instant) return DATE;
		return VARCHAR;
	}

	private ResultSet selectTags() throws SQLException {
		return statementProvider.get("select-tags").executeQuery();
	}

	private ResultSet selectInstants() throws SQLException {
		return statementProvider.get("select-instants").executeQuery();
	}

	private ResultSet selectLongValue(int tag, int feed) throws SQLException {
		PreparedStatement statement = statementProvider.get("select-long-value");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		return statement.executeQuery();
	}

	private ResultSet selectTextValue(int tag, int feed) throws SQLException {
		PreparedStatement statement = statementProvider.get("select-text-value");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		return statement.executeQuery();
	}

	private ResultSet selectLongValues(int tag, int from, int to) throws SQLException {
		PreparedStatement statement = statementProvider.get("select-long-values");
		statement.setInt(1, tag);
		statement.setInt(2, from);
		statement.setInt(3, to);
		return statement.executeQuery();
	}

	private ResultSet selectTextValues(int tag, int from, int to) throws SQLException {
		PreparedStatement statement = statementProvider.get("select-text-values");
		statement.setInt(1, tag);
		statement.setInt(2, from);
		statement.setInt(3, to);
		return statement.executeQuery();
	}

	private void updateFeed() throws SQLException {
		PreparedStatement statement = statementProvider.get("update-feed");
		statement.setLong(1, ++feeds);
		statement.execute();
	}

	private void insertTag(int id, String tag) throws SQLException {
		PreparedStatement statement = statementProvider.get("insert-tag");
		statement.setInt(1, id);
		statement.setString(2, tag);
		statement.execute();
	}

	private void insertEntry(int tag, Instant value) throws SQLException {
		PreparedStatement statement = statementProvider.get("insert-entry");
		statement.setInt(1, tag);
		statement.setLong(2, feeds);
		statement.setLong(3, value.toEpochMilli());
		statement.setString(4, labelOf(value));
		statement.execute();
	}

	private void insertEntry(int tag, long value) throws SQLException {
		PreparedStatement statement = statementProvider.get("insert-entry");
		statement.setInt(1, tag);
		statement.setLong(2, feeds);
		statement.setLong(3, value);
		statement.setNull(4, VARCHAR);
		statement.execute();
	}

	private void insertEntry(int tag, String value) throws SQLException {
		PreparedStatement statement = statementProvider.get("insert-entry");
		statement.setInt(1, tag);
		statement.setLong(2, feeds);
		statement.setNull(3, BIGINT);
		statement.setString(4, value);
		statement.execute();
	}

	private void updateTagFeed(int id) throws SQLException {
		PreparedStatement statement = statementProvider.get("update-tag-feed");
		statement.setInt(1, feeds);
		statement.setInt(2, id);
		statement.execute();
	}

	@Override
	public void close()  {
		try {
			this.connection.close();
		} catch (SQLException e) {
			throw new RegistryException(e);
		}
	}

	private static String labelOf(Instant value) {
		if (value.equals(Legacy)) return "Legacy";
		if (value.equals(BigBang)) return "Big Bang";
		return value.toString().substring(0, 19).replace('T', ' ');
	}

	private static void loadDriver() {
		try {
			DriverManager.getConnection("jdbc:sqlite::memory:").close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static class ConnectionProvider {

		private static final String CreateMapDefinition = """
					CREATE TABLE tags (
						tag INTEGER NOT NULL,
						label TEXT,
						feed INTEGER,
						PRIMARY KEY (tag)
					);
				
					CREATE TABLE map (
						tag INTEGER NOT NULL,
						feed INTEGER NOT NULL,
						num BIGINT,
						txt TEXT,
						PRIMARY KEY (tag, feed)
					);
					CREATE INDEX IF NOT EXISTS idx_tag ON map(tag);
					CREATE INDEX IF NOT EXISTS idx_feed ON map(feed);
					INSERT INTO tags (tag, label) VALUES (0, 'ts');
					INSERT INTO tags (tag, label) VALUES (1, 'ss');
					INSERT INTO map (tag, feed, num, txt) VALUES (-1, -1, 0, NULL);
				""";

		static Connection open(File file) throws SQLException {
			Connection connection = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
			connection.setAutoCommit(false);
			return connection;
		}

		static Connection create(File file) throws SQLException {
			Connection connection = open(file);
			Statement statement = connection.createStatement();
			statement.executeUpdate(CreateMapDefinition);
			connection.commit();
			return connection;
		}
	}

	class StatementProvider {
		private final Map<String, PreparedStatement> statements;

		StatementProvider() throws SQLException {
			this.statements = statements();
		}

		Map<String, PreparedStatement> statements() throws SQLException {
			Map<String, PreparedStatement> statements = new HashMap<>();
			statements.put("insert-tag", create("INSERT INTO tags (tag, label, feed) VALUES (?, ?, -1)"));
			statements.put("insert-entry", create("INSERT INTO map (tag, feed, num, txt) VALUES (?, ?, ?, ?)"));
			statements.put("update-tag-feed", create("UPDATE tags SET feed = ? WHERE tag = ?;"));
			statements.put("update-feed", create("UPDATE map SET num = ? WHERE tag = -1 AND feed = -1;"));
			statements.put("select-tags", create("SELECT tag, label, feed FROM tags"));
			statements.put("select-instants", create("SELECT feed, num FROM map WHERE tag = 0"));
			statements.put("select-long-value", create("SELECT num FROM map WHERE tag = ? AND feed = ?"));
			statements.put("select-long-values", create("SELECT feed, num FROM map WHERE tag = ? and feed BETWEEN ? AND ?"));
			statements.put("select-text-value", create("SELECT txt FROM map WHERE tag = ? AND feed = ?"));
			statements.put("select-text-values", create("SELECT feed, txt FROM map WHERE tag = ? and feed BETWEEN ? AND ?"));
			return statements;
		}

		PreparedStatement get(String sql) {
			return statements.get(sql);
		}

		private PreparedStatement create(String sql) throws SQLException {
			return connection.prepareStatement(sql);
		}

	}

	class TagSet {
		private final Map<String, Integer> labels;
		private final Map<Integer, Integer> lastUpdatingFeeds;

		TagSet() throws SQLException {
			this.labels = new HashMap<>();
			this.lastUpdatingFeeds = new HashMap<>();
			this.init(selectTags());
		}

		int get(String tag) {
			return labels.get(tag);
		}

		List<String> tags() {
			return labels.entrySet().stream()
					.filter(e -> e.getValue() > 1)
					.sorted(comparingInt(Map.Entry::getValue))
					.map(Map.Entry::getKey)
					.toList();
		}

		int size() {
			return labels.size();
		}

		int lastUpdatingFeedOf(int tag) {
			return lastUpdatingFeeds.getOrDefault(tag, -1);
		}

		void insert(Set<String> tags) throws SQLException {
			for (String tag : tags) insert(tag);
		}

		void update(Set<String> tags, Instant instant) throws SQLException {
			for (String tag : tags) update(get(tag), instant);
		}

		private void init(ResultSet rs) throws SQLException {
			try (rs) {
				while (rs.next())
					init(rs.getInt(1), rs.getString(2), rs.getInt(3));
			}
		}

		private void init(int id, String label, int feed) {
			labels.put(label, id);
			lastUpdatingFeeds.put(id, feed);
		}

		private void insert(String tag) throws SQLException {
			if (labels.containsKey(tag)) return;
			int index = size();
			labels.put(tag, index);
			insertTag(index, tag);
		}

		private void update(int tag, Instant instant) throws SQLException {
			if (lastUpdatingInstantOf(tag).isAfter(instant)) return;
			lastUpdatingFeeds.put(tag, feeds);
			updateTagFeed(tag);
		}

		private Instant lastUpdatingInstantOf(int tag) {
			return lastUpdatingFeeds.containsKey(tag) ?
					timeline.get(lastUpdatingFeeds.get(tag)) :
					Instant.MIN;
		}

		public boolean contains(String tag) {
			return labels.containsKey(tag);
		}
	}

	class Timeline {
		private final Map<Integer, Instant> instants;

		Timeline() throws SQLException {
			this.instants = new HashMap<>();
			this.init(selectInstants());
		}

		Instant get(int feed) {
			return instants.get(feed);
		}

		List<Instant> instants() {
			return new ArrayList<>(instants.values()).stream()
					.sorted()
					.toList();
		}

		void update(Instant instant) {
			instants.put(feeds, instant);
		}

		private void init(ResultSet rs) throws SQLException {
			try (rs) {
				while (rs.next())
					instants.put(rs.getInt(1), Instant.ofEpochMilli(rs.getLong(2)));
			}
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
}
