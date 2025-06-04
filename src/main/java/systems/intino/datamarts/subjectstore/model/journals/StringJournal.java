package systems.intino.datamarts.subjectstore.model.journals;

import systems.intino.datamarts.subjectstore.model.Journal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class StringJournal implements Journal {
	private final List<Transaction> transactions;

	public StringJournal() {
		this.transactions = new ArrayList<>();
	}

	public StringJournal(String content) {
		this.transactions = new ArrayList<>(transactionsIn(content));
	}

	@Override
	public boolean isEmpty() {
		return transactions.isEmpty();
	}

	@Override
	public List<Transaction> transactions() {
		return transactions;
	}


	private List<Transaction> transactionsIn(String content) {
		if (content.isEmpty()) return List.of();
		return Arrays.stream(content.split("\n"))
				.map(Transaction::of)
				.toList();
	}

	@Override
	public void add(Transaction transaction) {
		transactions.add(transaction);
	}

	@Override
	public String toString() {
		return transactions.stream().map(Transaction::toString).collect(Collectors.joining("\n"));
	}
}
