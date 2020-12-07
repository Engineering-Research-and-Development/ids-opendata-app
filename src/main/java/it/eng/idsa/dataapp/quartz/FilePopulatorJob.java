package it.eng.idsa.dataapp.quartz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;

import it.eng.idsa.dataapp.exception.WriteFileLockedException;
import it.eng.idsa.dataapp.service.FileWritterService;

public class FilePopulatorJob implements Job {

	private static final Logger logger = LogManager.getLogger(FilePopulatorJob.class);

	@Autowired
	private FileWritterService fileWritterService;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		logger.info("Triggered fileWritterService.writeToSourceFile");
		try {
			fileWritterService.writeToSourceFile();
		} catch (WriteFileLockedException e) {
			logger.error("Error while executing trigger for fileWritterService");
		}
	}

}
