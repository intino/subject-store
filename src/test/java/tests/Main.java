package tests;

import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.SubjectHistoryView;
import systems.intino.datamarts.subjectstore.calculator.model.filters.CosFilter;
import systems.intino.datamarts.subjectstore.calculator.model.filters.LagFilter;
import systems.intino.datamarts.subjectstore.calculator.model.filters.LeadFilter;
import systems.intino.datamarts.subjectstore.calculator.model.filters.SinFilter;

import java.io.*;
import java.time.Duration;

import static java.time.temporal.ChronoUnit.HOURS;

public class Main {
	public static void main(String[] args) throws IOException {
		SubjectHistory history = new SubjectHistory("Infecar-Twin", "jdbc:sqlite:infecar.ddb");
		long l = System.nanoTime();
		SubjectHistoryView view = SubjectHistoryView.of(history)
				.from(history.first().truncatedTo(HOURS))
				.to(history.last().truncatedTo(HOURS))
				.duration(Duration.ofHours(1))
				.add("hour-sin", "ts.hour-of-day", new SinFilter())
				.add("hour-cos", "ts.hour-of-day", new CosFilter())
				.add("day-sin", "ts.day-of-week", new SinFilter())
				.add("day-cos", "ts.day-of-week", new CosFilter())
				.add("month-sin", "ts.month-of-year", new SinFilter())
				.add("month-cos", "ts.month-of-year", new CosFilter())
				.add("generation-active-power", "generation_activePower.first")
				.add("generation-active-power-lead", "generation-active-power", new LagFilter(1))
				.add("consumption-active-power", "consumption_activePower.first")
				.add("consumption-active-power-lead", "consumption_activePower.first", new LeadFilter(2))
				.add("cell-temperature", "operational_cellTemperature.first")
				.add("radiation", "weather_radiation.first")
				.add("temperature", "weather_temperature.first")
				.build();
		try (BufferedOutputStream output = open("infecar.tsv")) {
			view.export().start(2).stop(1).to(output);
		}
		System.out.println((System.nanoTime() - l) / 1e9);
	}

	private static BufferedOutputStream open(String name) throws FileNotFoundException {
		return new BufferedOutputStream(new FileOutputStream(name));
	}
}
