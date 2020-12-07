package it.eng.idsa.dataapp.service.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import de.fraunhofer.iais.eis.Message;
import it.eng.idsa.dataapp.exception.EmptyFileException;
import it.eng.idsa.dataapp.exception.ReadFileLockedException;
import it.eng.idsa.dataapp.service.FileReaderService;

@Service
public class FileReaderServiceImpl implements FileReaderService {
	
	private static final Logger logger = LogManager.getLogger(FileReaderService.class);

	public static final String READ_LOCK_FILE = "read.lock";
	
	@Value("${application.dataLakeDirectory}")
	private Path dataLakeDirectory;
	
	@Override
	public String readRequestedArtifact(Message requestMessage, String requestedArtifact) 
			throws IOException, ReadFileLockedException, EmptyFileException {
		
		logger.info("Reading file from file system: " + requestedArtifact);
		String encodedFile = null;
		createReadLock();
		InputStream is = null;
		try {
			is = Files.newInputStream(dataLakeDirectory.resolve(requestedArtifact));
			if(dataLakeDirectory.resolve(requestedArtifact).toFile().length() == 0) {
				logger.info("Artifact with name {} is empty", dataLakeDirectory.resolve(requestedArtifact).toString());
				throw new EmptyFileException(String.format("File %s is empty", 
						dataLakeDirectory.resolve(requestedArtifact).toString()));
			}
			String message = IOUtils.toString(is, "UTF8");
			encodedFile = Base64.getEncoder().encodeToString(message.getBytes());
			logger.info("File read from disk.");
		} finally {
			if (is != null) {
				is.close();
			}
			releaseReadLock();
		}
		return encodedFile;
	}

	private void createReadLock() throws ReadFileLockedException {
		try {
			Files.write(Paths.get(READ_LOCK_FILE), new ArrayList<>(), StandardCharsets.UTF_8);
		} catch (IOException e) {
			logger.error("Could not create read lock file {}, {}", READ_LOCK_FILE, e);
			throw new ReadFileLockedException("Could not create read.lock");
		}
	}

	private void releaseReadLock() {
		File f = new File(READ_LOCK_FILE);
		if (f.isFile()) {
			logger.info("{} exists - deleting it...", READ_LOCK_FILE);
			f.delete();
		}
	}
}
