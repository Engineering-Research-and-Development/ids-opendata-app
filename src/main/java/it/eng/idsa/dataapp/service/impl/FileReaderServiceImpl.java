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
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import de.fraunhofer.iais.eis.Message;
import it.eng.idsa.dataapp.exception.ReadFileLockedException;
import it.eng.idsa.dataapp.service.FileReaderService;
import it.eng.idsa.dataapp.service.FileWritterService;

@Service
public class FileReaderServiceImpl implements FileReaderService {
	
	private static final Logger logger = LogManager.getLogger(FileReaderService.class);

	public static final String READ_LOCK_FILE = "read.lock";
	
	@Autowired
	private FileWritterService filefWritterService;

	@Value("${application.ckan.writeFileLock}")
	private String writeLockFile;
	
	@Value("${application.dataLakeDirectory}")
	private Path dataLakeDirectory;
	
	@Override
	@Retryable(value = ReadFileLockedException.class, maxAttempts = 3, backoff = @Backoff(delay = 2000))
	public String readRequestedArtifact(Message requestMessage, String requestedArtifact) {
		String responseMessageString = null;
		try {
			responseMessageString = readFile(requestedArtifact);
		} catch (IOException | ReadFileLockedException e) {
			logger.error(e);
		}
		return responseMessageString;
	}

	private String readFile(String requestedArtifact) throws IOException, ReadFileLockedException {
		logger.info("Reading file from disk (classPath) " + requestedArtifact);
		String encodedFile = null;
		createReadLock();
		InputStream is = null;
		try {
			is = Files.newInputStream(dataLakeDirectory.resolve(requestedArtifact));
			String message = IOUtils.toString(is, "UTF8");
			if(StringUtils.isEmpty(message)) {
				logger.info("No data present - send only header");
				message = filefWritterService.getHeaderLine();
			}
			encodedFile = Base64.getEncoder().encodeToString(message.getBytes());
			emptySourceFile(dataLakeDirectory.resolve(requestedArtifact));
			releaseReadLock();
			logger.info("File read from disk.");
		} finally {
			if (is != null) {
				is.close();
			}
		}
		return encodedFile;
	}

	private void emptySourceFile(Path file) {
		logger.info("Emptying file");
		try {
			Files.write(file, new ArrayList<>(), StandardCharsets.UTF_8);
		} catch (IOException e) {
			logger.error("Error while trying to empty source file", e);
		}
	}

	private void createReadLock() throws ReadFileLockedException {
		File f = new File(writeLockFile);
		if (!f.isFile()) {
			logger.info("No write lock file present, creating read lock");
			try {
				Files.write(Paths.get(READ_LOCK_FILE), new ArrayList<>(), StandardCharsets.UTF_8);
			} catch (IOException e) {
				logger.error("Could not create read lock file {}, {}", READ_LOCK_FILE, e);
				throw new ReadFileLockedException("Could not create read.lock");
			}
		} else {
			logger.info("Write lock file present");
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
