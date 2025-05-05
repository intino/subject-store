package systems.intino.datamarts.subjectstore.model;

public record Triple(Subject subject, Term term) {

	public Triple(String str) {
		this(str.split("\t"));
	}

	public Triple(String subject, String tag, String value) {
		this(Subject.of(subject), new Term(tag, value));
	}

	private Triple(String[] split) {
		this(split[0], split[1], split[2]);
	}

	@Override
	public String toString() {
		return subject + "\t" + term.tag() + "\t" + term.value();
	}
}
