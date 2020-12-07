package it.eng.idsa.dataapp.service;

import java.io.IOException;

/**
 * Service responsible for uplaoding file to Open Data service</br>
 * Idea is to have various implementations for service, based on configuration/property
 * @author igor.balog
 *
 */
public interface OpenDataService {
	
	void uploadData(String fileName) throws IOException;
}
