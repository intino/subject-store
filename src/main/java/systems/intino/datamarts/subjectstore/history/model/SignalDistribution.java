package systems.intino.datamarts.subjectstore.history.model;

import com.tdunning.math.stats.TDigest;

public class SignalDistribution {
	private final TDigest tdigest;

	public static SignalDistribution of(Iterable<Point<Double>> points) {
		SignalDistribution distribution = new SignalDistribution();
		for (Point<Double> point : points)
			distribution.tdigest.add(point.value());
		distribution.tdigest.compress();
		return distribution;
	}

	private SignalDistribution() {
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
