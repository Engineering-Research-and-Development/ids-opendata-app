package it.eng.idsa.dataapp.service;

import java.io.IOException;

public interface CKANService {
	
	void sendFileToCkan(String fileName) throws IOException;
}
