package io.intino.alexandria.model.series.signal;

import com.tdunning.math.stats.TDigest;
import io.intino.alexandria.model.Point;

public class Distribution {
	private final TDigest tdigest;

	public static Distribution of(Iterable<Point<Long>> points) {
		Distribution distribution = new Distribution();
		for (Point<Long> point : points)
			distribution.tdigest.add(point.value());
		distribution.tdigest.compress();
		return distribution;
	}

	private Distribution() {
		this.tdigest = TDigest.createAvlTreeDigest(200);
	}

	public long count() {
		return tdigest.size();
	}

	public double min() {
		return tdigest.getMin();
	}

	public double max() {
		return tdigest.getMax();
	}

	public double probabilityLeftTail(double value) {
		return tdigest.cdf(value);
	}

	public double probabilityRightTail(double value) {
		return 1-tdigest.cdf(value);
	}

	public double quantile(double value) {
		return tdigest.quantile(value);
	}

	public double q1() {
		return quantile(0.25);
	}

	public double q2() {
		return quantile(0.5);
	}

	public double median() {
		return quantile(0.5);
	}

	public double q3() {
		return quantile(0.75);
	}

}
