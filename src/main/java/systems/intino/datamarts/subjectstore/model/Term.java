package systems.intino.datamarts.subjectstore.model;

public record Term(String tag, String value) {

	public static Term of(String str) {
		if (str == null || str.isEmpty()) return null;
		String[] split = str.split("=",2);
		return new Term(split[0], split[1]);
	}

	public Term {
		tag = tag.trim();
		value = value.trim();
	}

	@Override
	public String toString() {
		return tag + "=" + value;
	}

	public boolean is(String tag) {
		return tag.equals(this.tag);
	}
}
