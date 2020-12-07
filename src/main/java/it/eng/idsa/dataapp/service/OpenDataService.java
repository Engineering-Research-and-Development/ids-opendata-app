package it.eng.idsa.dataapp.service;

import java.io.IOException;

public interface OpenDataService {
	
	void sendFileToCkan(String fileName) throws IOException;
}
