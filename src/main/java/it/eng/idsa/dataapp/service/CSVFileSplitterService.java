package it.eng.idsa.dataapp.service;

import java.io.IOException;

public interface CSVFileSplitterService {

	/**
	 * Splits original CSF file and return number of pars created</br>
	 * It appends _part + counter at the end of original file name
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	 int splitCSV(String fileName) throws IOException;
	 
	 /**
	  * Create file part name based path, original file name and part index
	  * @param fileName
	  * @param part
	  * @return
	  */
	 String createFilePartName(String fileName, int part);
}
