package systems.intino.datamarts.subjectstore.model.journals;

import systems.intino.datamarts.subjectstore.model.Journal;

import java.util.Arrays;
import java.util.List;

public class StringJournal implements Journal {
	private final List<String> lines;

	public StringJournal(String content) {
		this.lines = Arrays.stream(content.split("\n")).toList();
	}

	@Override
	public boolean isEmpty() {
		return lines.isEmpty();
	}

	@Override
	public List<Transaction> transactions() {
		return lines.stream()
				.map(Transaction::of)
				.toList();
	}

	@Override
	public String toString() {
		return String.join("\n", lines);
	}
}
