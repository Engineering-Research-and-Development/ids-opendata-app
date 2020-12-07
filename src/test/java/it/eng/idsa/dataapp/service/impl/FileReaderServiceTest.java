package it.eng.idsa.dataapp.service.impl;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;


import de.fraunhofer.iais.eis.ArtifactRequestMessage;
import de.fraunhofer.iais.eis.ArtifactResponseMessageBuilder;
import de.fraunhofer.iais.eis.Message;
import it.eng.idsa.dataapp.exception.ReadFileLockedException;
import it.eng.idsa.dataapp.service.FileWritterService;
import it.eng.idsa.dataapp.service.MultiPartMessageService;
import it.eng.idsa.multipart.processor.MultipartMessageProcessor;
import it.eng.idsa.multipart.util.DateUtil;

public class FileReaderServiceTest {
	
	private static final Logger logger = LogManager.getLogger(FileReaderServiceTest.class);

	private static final String MOCK_MESSAGE_STRING = "mocked message string";
	private static final String MOCK_MESSAGE_HEADER = "mocked message header";
	private static final String REQUESTED_ARTIFACT = "test.csv";
	private static final String WRITE_LOCK_FILE = "write.lock";
	
	@InjectMocks
	private FileReaderServiceImpl service;
	
	@Mock
	private MultiPartMessageService multiPartMessageService;
	@Mock
	private ArtifactRequestMessage artifactRequestMessage;
	@Mock
	private FileWritterService fileWritterService;
	
	private String artifactResponseMessageString;

	@BeforeEach
	public void setup() throws IOException {
		MockitoAnnotations.initMocks(this);
		ReflectionTestUtils.setField(service, "dataLakeDirectory", Paths.get("src","test","resources"));
		ReflectionTestUtils.setField(service, "writeLockFile", WRITE_LOCK_FILE);
		populateTestFile();
		clearLockFiles();
		
		when(multiPartMessageService.getMessage(MOCK_MESSAGE_STRING)).thenReturn(artifactRequestMessage);
		when(multiPartMessageService.getHeader(MOCK_MESSAGE_STRING)).thenReturn(MOCK_MESSAGE_HEADER);
		when(artifactRequestMessage.getRequestedArtifact()).thenReturn(URI.create("http://test.com/artifact/" + REQUESTED_ARTIFACT));
		
		createArtifactResponseString();
		when(multiPartMessageService.getResponseHeader(artifactRequestMessage)).thenReturn(artifactResponseMessageString);

	}
	
	@AfterAll
	public static void cleanupLockFiles() {
		clearLockFiles();
	}

	@Test
	public void testReadFile() throws IOException, ReadFileLockedException {
		String requestedArtifact = service.readRequestedArtifact(artifactRequestMessage, REQUESTED_ARTIFACT);
		assertNotNull(requestedArtifact);
	}
	
	@Test
	public void readArtifact_readLockPresent() throws IOException, ReadFileLockedException {
		createLockFile(FileReaderServiceImpl.READ_LOCK_FILE);

		String requestedArtifact = service.readRequestedArtifact(artifactRequestMessage, REQUESTED_ARTIFACT);
		assertNotNull(requestedArtifact);
	}
	
	@Test
	public void readArtifact_writeLockPresent() throws IOException {
		createLockFile("write.lock");
		assertNull(service.readRequestedArtifact(artifactRequestMessage, REQUESTED_ARTIFACT));
	}
	
	@Test
	public void readArtifact_readFromEmptyFile() throws IOException, ReadFileLockedException {
		when(fileWritterService.getHeaderLine()).thenReturn("Header1, Header2, Header3");

		Files.write(Paths.get("src","test","resources", "test.csv"), new ArrayList<>(), StandardCharsets.UTF_8);
		
		String requestedArtifact = service.readRequestedArtifact(artifactRequestMessage, REQUESTED_ARTIFACT);
		assertNotNull(requestedArtifact);
		String expected = new String (Base64.getEncoder().encode("Header1, Header2, Header3".getBytes()));
		assertTrue(expected.equals(requestedArtifact));
	}
	
	private void populateTestFile() throws IOException {
		List<String> fileLines = new ArrayList<>();
		fileLines.add("test1,test1");
		fileLines.add("A,B");
		Files.write(Paths.get("src","test","resources","test.csv"), 
				fileLines, 
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, 
                StandardOpenOption.APPEND);
	}
	
	private static void clearLockFiles() {
		File readLock = new File("src/test/resources/read.lock");
		if (readLock.isFile()) {
			logger.info("Deleting read lock file");
			readLock.delete();
		}
		File writeLock = new File(WRITE_LOCK_FILE);
		if (writeLock.isFile()) {
			logger.info("Deleting write lock file");
			writeLock.delete();
		}
	}
	
	private void createArtifactResponseString() throws IOException {
		Message message = new ArtifactResponseMessageBuilder()
				._issuerConnector_(URI.create("auto-generated"))
				._issued_(DateUtil.now())
				._modelVersion_("4.0.0")
				.build();
		artifactResponseMessageString = MultipartMessageProcessor.serializeToPlainJson(message);
	}
	
	private void createLockFile(String lockFileName) throws IOException {
		logger.info("Creating lock file - {}", lockFileName);
//		"src", "test", "resources"
		Files.write(Paths.get(lockFileName), 
				new ArrayList<>(), 
				StandardCharsets.UTF_8,
				StandardOpenOption.CREATE, 
				StandardOpenOption.APPEND);
	}
}
