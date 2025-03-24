package io.intino.alexandria.io;

import io.intino.alexandria.model.Point;

import java.io.Closeable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.YEARS;

public interface Registry extends Closeable {

	String name();

	int feeds();

	List<String> tags();

	List<Instant> instants();

	String ss(int feed);

	Point<Long> readLong(String tag);

	Point<String> readText(String tag);

	List<Point<Long>> readLongs(String tag, Instant from, Instant to);

	List<Point<String>> readTexts(String tag, Instant from, Instant to);

	void register(Transaction transaction);

}
