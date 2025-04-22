package systems.intino.datamarts.subjectstore.model;

public record Statement(Subject subject, Term term) {

	public Statement(String str) {
		this(x(str).split("\t"));
	}

	private static String x(String str) {
		return str;
	}

	public Statement(String subject, String tag, String value) {
		this(Subject.of(subject), new Term(tag, value));
	}

	private Statement(String[] split) {
		this(split[0], split[1], split[2]);
	}

	@Override
	public String toString() {
		return subject + "\t" + term.tag() + "\t" + term.value();
	}
}
