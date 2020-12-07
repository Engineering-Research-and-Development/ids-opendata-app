package it.eng.idsa.dataapp.service.impl;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
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
import it.eng.idsa.dataapp.exception.EmptyFileException;
import it.eng.idsa.dataapp.exception.ReadFileLockedException;
import it.eng.idsa.dataapp.service.MultiPartMessageService;
import it.eng.idsa.multipart.processor.MultipartMessageProcessor;
import it.eng.idsa.multipart.util.DateUtil;

public class FileReaderServiceTest {
	
	private static final Logger logger = LogManager.getLogger(FileReaderServiceTest.class);

	private static final String MOCK_MESSAGE_STRING = "mocked message string";
	private static final String MOCK_MESSAGE_HEADER = "mocked message header";
	private static final String REQUESTED_ARTIFACT = "test.csv";
	
	@InjectMocks
	private FileReaderServiceImpl service;
	
	@Mock
	private MultiPartMessageService multiPartMessageService;
	@Mock
	private ArtifactRequestMessage artifactRequestMessage;
	
	private String artifactResponseMessageString;

	@BeforeEach
	public void setup() throws IOException {
		MockitoAnnotations.initMocks(this);
		ReflectionTestUtils.setField(service, "dataLakeDirectory", Paths.get("src","test","resources"));
		clearFiles();
		populateTestFile();
		
		when(multiPartMessageService.getMessage(MOCK_MESSAGE_STRING)).thenReturn(artifactRequestMessage);
		when(multiPartMessageService.getHeader(MOCK_MESSAGE_STRING)).thenReturn(MOCK_MESSAGE_HEADER);
		when(artifactRequestMessage.getRequestedArtifact()).thenReturn(URI.create("http://test.com/artifact/" + REQUESTED_ARTIFACT));
		
		createArtifactResponseString();
		when(multiPartMessageService.getResponseHeader(artifactRequestMessage)).thenReturn(artifactResponseMessageString);

	}
	
	@AfterAll
	public static void cleanupLockFiles() {
		clearFiles();
	}

	@Test
	public void testReadFile() throws IOException, ReadFileLockedException, EmptyFileException {
		String requestedArtifact = service.readRequestedArtifact(artifactRequestMessage, REQUESTED_ARTIFACT);
		assertNotNull(requestedArtifact);
	}
	
	@Test
	public void readArtifact_readLockPresent() throws IOException, ReadFileLockedException, EmptyFileException {
		createLockFile(FileReaderServiceImpl.READ_LOCK_FILE);

		String requestedArtifact = service.readRequestedArtifact(artifactRequestMessage, REQUESTED_ARTIFACT);
		assertNotNull(requestedArtifact);
	}
	
	@Test
	public void readArtifact_fileNotFound() throws IOException, ReadFileLockedException {
		assertThrows(NoSuchFileException.class, 
				() -> service.readRequestedArtifact(artifactRequestMessage, "not_found.csv"));
	}
	
	@Test
	public void readArtifact_readFromEmptyFile() throws IOException, ReadFileLockedException, EmptyFileException {
		Files.write(Paths.get("src","test","resources", REQUESTED_ARTIFACT), new ArrayList<>(), StandardCharsets.UTF_8);
		
		assertThrows(EmptyFileException.class, 
				() -> service.readRequestedArtifact(artifactRequestMessage, REQUESTED_ARTIFACT));
	}
	
	private void populateTestFile() throws IOException {
		List<String> fileLines = new ArrayList<>();
		fileLines.add("test1,test1");
		fileLines.add("A,B");
		Files.write(Paths.get("src","test","resources", REQUESTED_ARTIFACT), 
				fileLines, 
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, 
                StandardOpenOption.APPEND);
	}
	
	private static void clearFiles() {
		File readLock = new File("src/test/resources/read.lock");
		if (readLock.isFile()) {
			logger.info("Deleting read lock file");
			readLock.delete();
		}
		File testCSV = new File("src/test/resources/" + REQUESTED_ARTIFACT);
		if (testCSV.isFile()) {
			logger.info("Deleting test csv file");
			testCSV.delete();
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
