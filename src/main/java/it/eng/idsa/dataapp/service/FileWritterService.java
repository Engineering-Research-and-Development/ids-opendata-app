package it.eng.idsa.dataapp.service;

import java.io.IOException;

import it.eng.idsa.dataapp.exception.WriteFileLockedException;

public interface FileWritterService {
	
	/**
	 * Read from source csv file and writes to destination csv file configured number of lines
	 */
	void writeToSourceFile() throws WriteFileLockedException;
	
	String getHeaderLine() throws IOException;
	
	void resetStartLine();
}
