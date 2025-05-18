package tests;

import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.SubjectHistoryView;
import systems.intino.datamarts.subjectstore.calculator.model.filters.CosFilter;
import systems.intino.datamarts.subjectstore.calculator.model.filters.LagFilter;
import systems.intino.datamarts.subjectstore.calculator.model.filters.LeadFilter;
import systems.intino.datamarts.subjectstore.calculator.model.filters.SinFilter;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;

import static java.time.temporal.ChronoUnit.HOURS;

public class Main {
	public static void main(String[] args) throws SQLException, IOException {
		Connection connection = DriverManager.getConnection("jdbc:sqlite:infecar.ddb");
		connection.setAutoCommit(false);
		SubjectHistory history = new SubjectHistory("Infecar-Twin", connection);
		OutputStream os = new BufferedOutputStream(new FileOutputStream("infecar.tsv"));
		SubjectHistoryView.of(history)
				.from(history.first().truncatedTo(HOURS))
				.to(history.last().truncatedTo(HOURS))
				.period(Duration.ofHours(1))
				.add("hour-sin", "ts.hour-of-day", new SinFilter())
				.add("hour-cos", "ts.hour-of-day", new CosFilter())
				.add("month-sin", "ts.month-of-year", new SinFilter())
				.add("month-cos", "ts.month-of-year", new CosFilter())
				.add("consumption-active-power", "consumption_activePower.first")
				.add("consumption-reactive-power", "consumption_reactivePower.first")
				.add("generation-active-power", "generation_activePower.first")
				.add("generation-reactive-power", "generation_reactivePower.first")
				.add("weather-temperature", "weather_temperature.first")
				.add("weather-radiation", "weather_radiation.first")
				.add("cell-temperature", "operational_cellTemperature.first")
				.add("consumption-active-power-1", "consumption-active-power", new LagFilter(1))
				.add("consumption-reactive-power-1", "consumption-reactive-power", new LagFilter(1))
				.add("consumption-active-power+1", "consumption-active-power", new LeadFilter(1))
				.add("consumption-reactive-power+1", "consumption-reactive-power", new LeadFilter(1))
				.export().onlyCompleteRows().to(os);


	}
}
