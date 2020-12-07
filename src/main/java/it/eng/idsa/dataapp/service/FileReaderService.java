package it.eng.idsa.dataapp.service;

import java.io.IOException;

import de.fraunhofer.iais.eis.Message;
import it.eng.idsa.dataapp.exception.ReadFileLockedException;

public interface FileReaderService {
	
	String  readRequestedArtifact(Message requestMessage, String requestedArtifact) throws IOException, ReadFileLockedException;
}
