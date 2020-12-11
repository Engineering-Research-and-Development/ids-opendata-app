package it.eng.idsa.dataapp.service.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

import it.eng.idsa.dataapp.exception.WriteFileLockedException;
import it.eng.idsa.dataapp.service.FileWritterService;

@Service
public class FileWritterServiceImpl implements FileWritterService {

	private static final Logger logger = LogManager.getLogger(FileWritterService.class);
	
	public static final String READ_LOCK_FILE = "read.lock";

	@Value("${application.opendata.ckan.writeFileLock}")
	private String writeLockFile;

	@Value("${application.dataLakeDirectory}")
	private Path dataLakeDirectory;

	@Value("${application.opendata.ckan.sourceFile}")
	private String sourceFileName;

	@Value("${application.opendata.ckan.destinationFile}")
	private String destinationFile;

	@Value("${application.opendata.ckan.numberOfLinesToWrite}")
	private int numberOfLinesToWrite;

	@Value("${application.opendata.ckan.appendToDestinationFile:false}")
	private boolean appendToDestinationFile;
	
	private int startLine = 0;

	private String[] headerRow;

	@Override
	@Retryable(value = WriteFileLockedException.class, maxAttempts = 3, backoff = @Backoff(delay = 2000))
	public void writeToSourceFile() throws WriteFileLockedException {
		createWriteLock();
		try {
			List<String[]> csvLinesToWrite = new ArrayList<>();
			csvLinesToWrite = readLines();
			csvWriterAll(csvLinesToWrite, dataLakeDirectory.resolve(destinationFile));
			releaseWriteLock();
		} catch (Exception e) {
			logger.error("Error while creating CSVReader", e);
		}
	}

	/**
	 * Read lines from source csv file</br>
	 * In first run, read header and store it for writing
	 * @return
	 * @throws IOException
	 */
	public List<String[]> readLines() throws IOException {
		logger.info("Read from source csv file, lines to skip {}", startLine);
		Reader reader = Files.newBufferedReader(dataLakeDirectory.resolve(sourceFileName));
		List<String[]> list = new ArrayList<>();
		try {
			CSVReader csvReader = new CSVReaderBuilder(reader)
					.withSkipLines(startLine)
					.build();

			int readLine = 0;
			String[] line;
			if(startLine == 0) {
				headerRow = csvReader.readNext();
				// calculate header row
				startLine++;
			}
			while ((line = csvReader.readNext()) != null && readLine < numberOfLinesToWrite) {
				list.add(line);
				readLine++;
			}
			startLine = startLine + numberOfLinesToWrite;
			reader.close();
			csvReader.close();
			logger.info("Finished writing to target file, next start line is: {}", startLine);
		} catch (Exception ex) {
			logger.error(ex);
		}
		return list;
	}

	/**
	 * Write to target CSV file</br>
	 * If file size is 0 - write header first
	 * @param stringArray
	 * @param path
	 * @param clearFile should empty file or not
	 * @throws Exception
	 */
	private void csvWriterAll(List<String[]> stringArray, Path path) throws Exception {
		CSVWriter writer = new CSVWriter(
				new FileWriter(path.toString(), appendToDestinationFile), 
				CSVWriter.DEFAULT_SEPARATOR, 
				CSVWriter.NO_QUOTE_CHARACTER, 
				CSVWriter.DEFAULT_ESCAPE_CHARACTER, 
				CSVWriter.DEFAULT_LINE_END);
		if(path.toFile().length() == 0) {
			logger.debug("Destination file is 0 sized, inserting header row first");
			writer.writeNext(headerRow);
		}
		writer.writeAll(stringArray);
		writer.close();
	}

	/**
	 * Check if read.lock file exists, in order to create write.lock file,</br>
	 * otherwise - skip and do nothing
	 * @return
	 * @throws WriteFileLockedException 
	 */
	private void createWriteLock() throws WriteFileLockedException {
		File f = new File(READ_LOCK_FILE);
		if (!f.isFile()) {
			logger.info("No read lock file present, creating write lock");
			try {
				Files.write(Paths.get(writeLockFile), new ArrayList<>(), StandardCharsets.UTF_8);
			} catch (IOException e) {
				logger.error("Could not create write lock file {}, {}", writeLockFile, e);
				throw new WriteFileLockedException("Could not create write.lock");
			}
		} else {
			logger.info("Read lock file present - skipping");
			throw new WriteFileLockedException("Could not create write.lock");
		}
	}

	private void releaseWriteLock() {
		File f = new File(writeLockFile);
		if (f.isFile()) {
			logger.info("{} exists - deleting it...", writeLockFile);
			f.delete();
		}
	}

	@Override
	public String getHeaderLine() throws IOException {
		if(headerRow == null) {
			// read header row from file
			logger.info("Scheduler not started - reading header line from source file");
			Reader reader = null;
			try {
				reader = Files.newBufferedReader(dataLakeDirectory.resolve(sourceFileName));
				CSVReader csvReader = new CSVReaderBuilder(reader)
						.withSkipLines(startLine)
						.build();
				headerRow = csvReader.readNext();
			} catch (IOException e) {
				logger.error(e);
			} finally {
				if(reader != null) {
					reader.close();
				}
			}
		} 
		return String.join(",", headerRow);
	}

	@Override
	public void resetStartLine() {
		this.startLine = 0;
	}
}
