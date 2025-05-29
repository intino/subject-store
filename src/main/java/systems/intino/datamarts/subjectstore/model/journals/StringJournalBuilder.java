package systems.intino.datamarts.subjectstore.model.journals;

import systems.intino.datamarts.subjectstore.model.Journal;
import systems.intino.datamarts.subjectstore.model.Subject;
import systems.intino.datamarts.subjectstore.model.Term;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StringJournalBuilder implements Journal.Builder {
	private final Subject subject;

	public StringJournalBuilder(Subject subject) {
		this.subject = subject;
	}

	@Override
	public Journal to(List<Term> target) {
		return journalWith(deletionsFor(target) + insertionsFor(target));
	}

	private StringJournal journalWith(String sb) {
		return new StringJournal(sb.trim().replace("[$]", subject.identifier()));
	}

	private String deletionsFor(List<Term> target) {
		return deletionsFor(new HashSet<>(target));
	}

	private String deletionsFor(Set<Term> target) {
		StringBuilder sb = new StringBuilder();
		for (Term term : subject.terms()) {
			if (target.contains(term)) continue;
			sb.append("del ").append("[$] ").append(term.serialize()).append('\n');
		}
		return sb.toString();
	}

	private String insertionsFor(List<Term> target) {
		Set<Term> current = new HashSet<>(subject.terms());
		StringBuilder sb = new StringBuilder();
		for (Term term : target) {
			if (current.contains(term)) continue;
			sb.append("put ").append("[$] ").append(term.serialize()).append('\n');
		}
		return sb.toString();
	}


}
