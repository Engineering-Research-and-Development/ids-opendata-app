package it.eng.idsa.dataapp.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import it.eng.idsa.dataapp.exception.WriteFileLockedException;

public class FileWritterServiceTest {

	@InjectMocks
	private FileWritterServiceImpl service;
	
	private static final String[] HEADER_LINE  = new String[]{"Number","Alpha","Name"};
	
	@BeforeEach
	public void setup() throws IOException {
		MockitoAnnotations.initMocks(this);
		
		ReflectionTestUtils.setField(service, "dataLakeDirectory", Paths.get("src","test","resources"));
		ReflectionTestUtils.setField(service, "writeLockFile", "write.lock");
		ReflectionTestUtils.setField(service, "sourceFileName", "source_file.csv");
		ReflectionTestUtils.setField(service, "destinationFile", "destination.csv");
		ReflectionTestUtils.setField(service, "numberOfLinesToWrite", 5);
	}
	
	@Test
	public void readFirstBatch() throws IOException {
		List<String[]> lines = service.readLines();
		
		assertEquals(5, lines.size());
		verifyDataLine(lines.get(0), "1", "A", "Fergus Mcgill");
		//5,E,Ivo Bullock - last line
		verifyDataLine(lines.get(4), "5", "E", "Ivo Bullock");
	}
	
	@Test
	public void read2TimesTest() throws IOException {
		List<String[]> lines = service.readLines();
		lines.addAll(service.readLines());
		verifyDataLine(lines.get(0), "1", "A", "Fergus Mcgill");
		verifyDataLine(lines.get(4), "5", "E", "Ivo Bullock");
		verifyDataLine(lines.get(5), "6", "F", "Aled Patel");
		verifyDataLine(lines.get(9), "10", "J", "Wade Rosario");
	}

	@Test
	public void readSecondBatch() throws IOException {
		ReflectionTestUtils.setField(service, "headerRow", HEADER_LINE);
		// 5 lines + header line
		ReflectionTestUtils.setField(service, "startLine", 6);
		List<String[]> lines = service.readLines();
		
		// 6 lines, header + 5 lines of data
		assertEquals(5, lines.size());
		// first data line - 6,F,Aled Patel
		verifyDataLine(lines.get(0), "6", "F", "Aled Patel");
		//last line - 10, J, Wade Rosario
		verifyDataLine(lines.get(4), "10", "J", "Wade Rosario");
	}
	
	@Test
	public void readThirdBatch() throws IOException {
		ReflectionTestUtils.setField(service, "headerRow", HEADER_LINE);
		ReflectionTestUtils.setField(service, "startLine", 11);

		List<String[]> lines = service.readLines();
		
		assertEquals(5, lines.size());
		// first line of data - 11,K,Ocean Dunlap
		verifyDataLine(lines.get(0), "11", "K", "Ocean Dunlap");
		// last line - 15,O,Delores Mac
		verifyDataLine(lines.get(4), "15", "O", "Delores Mac");
	}
	
	@Test
	public void writeSecondBatch() throws WriteFileLockedException {
		ReflectionTestUtils.setField(service, "headerRow", HEADER_LINE);
		ReflectionTestUtils.setField(service, "startLine", 6);

		service.writeToSourceFile();
	}
	
	@Test
	public void writeThirdBatch() throws WriteFileLockedException {
		ReflectionTestUtils.setField(service, "headerRow", HEADER_LINE);
		ReflectionTestUtils.setField(service, "startLine", 11);

		service.writeToSourceFile();
	}

	private void verifyDataLine(String[] dataLine, String firstColumn, String secondColumn, String thirdColumn) {
		assertEquals(firstColumn, dataLine[0]);
		assertEquals(secondColumn, dataLine[1]);
		assertEquals(thirdColumn, dataLine[2]);
	}
	
	
	private void verifyHeader(List<String[]> lines) {
		assertEquals(HEADER_LINE[0], lines.get(0)[0]);
		assertEquals(HEADER_LINE[1], lines.get(0)[1]);
		assertEquals(HEADER_LINE[2], lines.get(0)[2]);
	}
	
	
	@Test
	public void urlHandling() {
		URI uri = URI.create("http://example.com/foo/bar/fiz/test.csv?param=true");
		String path = uri.getPath();
		String idStr = path.substring(path.lastIndexOf('/') + 1);
		System.out.println(idStr);
	}
}
