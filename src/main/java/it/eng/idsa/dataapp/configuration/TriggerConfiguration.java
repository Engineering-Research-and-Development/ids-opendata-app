package it.eng.idsa.dataapp.configuration;

import static org.quartz.JobBuilder.newJob;

import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

import it.eng.idsa.dataapp.quartz.FilePopulatorJob;


@Configuration
@EnableRetry
@EnableScheduling
public class TriggerConfiguration {
	
	public static final String JOB_KEY = "Qrtz_Job_FileSplitter";

    @Bean
    public JobDetail jobDetail() {
        return newJob()
        		.ofType(FilePopulatorJob.class)
        		.storeDurably()
        		.withIdentity(JobKey.jobKey(JOB_KEY))
        		.withDescription("Invoke Sample Job service...")
        		.build();
    }
}
