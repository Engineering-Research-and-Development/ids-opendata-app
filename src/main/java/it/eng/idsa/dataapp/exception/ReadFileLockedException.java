package it.eng.idsa.dataapp.exception;

public class ReadFileLockedException extends Exception {

	private static final long serialVersionUID = 1L;

	public ReadFileLockedException(String message) {
		super(message);
	}
}
