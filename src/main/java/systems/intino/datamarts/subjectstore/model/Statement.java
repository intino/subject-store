package systems.intino.datamarts.subjectstore.model;

public record Statement(Subject subject, Term term) {

	public Statement(String str) {
		this(str.split("\t"));
	}

	public Statement(String subject, String term) {
		this(Subject.of(subject), Term.of(term));
	}

	private Statement(String[] split) {
		this(split[0], split[1]);
	}

	@Override
	public String toString() {
		return subject + "\t" + term;
	}
}
