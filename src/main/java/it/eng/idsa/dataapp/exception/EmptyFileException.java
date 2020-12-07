package it.eng.idsa.dataapp.exception;

public class EmptyFileException extends Exception {

	private static final long serialVersionUID = 1603749113588037479L;
	
	public EmptyFileException(String message) {
		super(message);
	}

}
