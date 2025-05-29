package systems.intino.datamarts.subjectstore.model;

import systems.intino.datamarts.subjectstore.helpers.EscapeSymbol;
import systems.intino.datamarts.subjectstore.model.journals.FileJournal;
import systems.intino.datamarts.subjectstore.model.journals.StringJournal;
import systems.intino.datamarts.subjectstore.model.journals.StringJournalBuilder;

import java.io.File;
import java.util.List;

public interface Journal {
	boolean isEmpty();

	List<Transaction> transactions();

	static Journal from(File file) {
		return new FileJournal(file);
	}

	static Journal from(String value) {
		return new StringJournal(value);
	}

	static Builder from(Subject subject) {
		return new StringJournalBuilder(subject);
	}

	interface Builder {
		Journal to(List<Term> terms);
	}

	record Transaction(Type type, String subject, String parameter) {
		public Transaction {
			parameter = escape(parameter);
		}

		@Override
		public String toString() {
			return type + " " + subject + " " + parameter;
		}

		private static String escape(String str) {
			return str.trim()
					.replace('\n', EscapeSymbol.NL)
					.replace('\t', EscapeSymbol.HT);
		}

		public static Transaction of(String line) {
			String[] split = line.split(" ", 3);
			return new Transaction(Type.valueOf(split[0]), split[1], split[2]);
		}


		public enum Type {
			put, set, del, drop, rename
		}
	}
}
