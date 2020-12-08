package it.eng.idsa.dataapp.web.rest;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import it.eng.idsa.dataapp.configuration.TriggerConfiguration;
import it.eng.idsa.dataapp.service.FileWritterService;

@RestController
@RequestMapping({ "/schedule" })
public class SchedulerController {
	
	private static final Logger logger = LogManager.getLogger(SchedulerController.class);

	@Autowired
	private Scheduler scheduler;
	
	@Autowired
	JobDetail jobDetail;
	
	@Autowired
	private FileWritterService fileWritterService;
	
	@Value("${application.quartz.triggerInterval}")
	private int triggerInterval;

	@PostMapping("/startScheduler")
	public ResponseEntity<String> startScheduler() {

		try {
			Trigger trigger =  TriggerBuilder.newTrigger()
	        		.forJob(jobDetail)
	        		.withIdentity(TriggerKey.triggerKey("Qrtz_Trigger_FileSplitter"))
	        		.withDescription("Trigger for csv file splitting job")
	        		.withSchedule(SimpleScheduleBuilder.simpleSchedule()
	        				.withIntervalInSeconds(triggerInterval)
	        				.repeatForever())
	        		.build();
			scheduler.scheduleJob(jobDetail, new HashSet<>(Arrays.asList(trigger)), true);
		} catch (SchedulerException e) {
			logger.error("Error while creating trigger for file splitting job", e);
			return new ResponseEntity<>("Failed to schedule process for splitting file", HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<>("Successfuly started schedule process for splitting file with interval (in seconds)" + triggerInterval, HttpStatus.OK);
	}
	
	@PostMapping("/removeScheduler")
	public ResponseEntity<String> stopScheduler()  {
		try {
			scheduler.deleteJob(JobKey.jobKey(TriggerConfiguration.JOB_KEY));
			fileWritterService.resetStartLine();
			logger.info("Removed job for sppliting csv file");
		} catch (SchedulerException e) {
			logger.error("Error while removing job for file splitting job", e);
			return new ResponseEntity<>("Failed to remove scheduled job for splitting file", HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<>("Successfuly removed schedule process for splitting file with interval", HttpStatus.OK);
	}
}
