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
	public static void main(String[] args) {
		String b = " hola mundo".substring(1).intern();
		String a = "--hola mundo".substring(2).intern();
		System.out.println(a == b);
	}
}
