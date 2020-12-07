package it.eng.idsa.dataapp.exception;

public class WriteFileLockedException extends Exception {

	private static final long serialVersionUID = 1L;

	public WriteFileLockedException(String message) {
		super(message);
	}
}
