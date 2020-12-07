package it.eng.idsa.dataapp.service.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import it.eng.idsa.dataapp.service.CSVFileSplitterService;

@Service
public class CSVFileSplitterServiceImpl implements CSVFileSplitterService {
	
	private static final Logger logger = LogManager.getLogger(CSVFileSplitterService.class);

	@Value("${application.ckan.maxFileSize}")
	private int maxFileSize;
	
	@Value("${application.dataLakeDirectory.destination}")
	private Path dataLakeDirectory;
	
	public int splitCSV(String fileName) throws IOException {
		logger.info("Started to split file {} to smaller parts", fileName);
		FileReader fileReader = new FileReader(dataLakeDirectory.resolve(fileName).toFile());
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line="";
		int fileSize = 0;
		BufferedWriter fos = new BufferedWriter(new FileWriter(createFilePartName(fileName, 1), true));
		int i=2;
		while((line = bufferedReader.readLine()) != null) {
		    if(fileSize + line.getBytes().length > maxFileSize){
		        fos.flush();
		        fos.close();
		        fos = new BufferedWriter(new FileWriter(createFilePartName(fileName, i),true));
		        fos.write(line + System.lineSeparator());
		        fileSize = line.getBytes().length;
		        i++;
		    }else{
		        fos.write(line + System.lineSeparator());
		        fileSize += line.getBytes().length;
		    }
		    
		}          
		fos.flush();
		fos.close();
		bufferedReader.close();
		int numberOfPartsSplitted = i-1;
		logger.info("Total number of smaller parts: {}", numberOfPartsSplitted);
		return numberOfPartsSplitted;
	}
	
	public String createFilePartName(String fileName, int part) {
		String extension = FilenameUtils.getExtension(fileName);
		String fileNameWithoutExtension = FilenameUtils.getBaseName(fileName);
		String partFileName = new StringBuilder(dataLakeDirectory.toString())
				.append(File.separator)
				.append(fileNameWithoutExtension)
				.append("_part")
				.append(part)
				.append(".")
				.append(extension)
				.toString();
		return partFileName;
	}
}
