package it.eng.idsa.dataapp.configuration;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

import it.eng.idsa.dataapp.quartz.FilePopulatorJob;


@Configuration
@ConditionalOnExpression("'${using.spring.schedulerFactory}'=='true'")
@EnableRetry
public class TriggerConfiguration {

	private static final Logger logger = LogManager.getLogger(TriggerConfiguration.class);
	
	@Value("${application.quartz.triggerInterval}")
	private int triggerInterval;
	
    @Bean
    public Trigger trigger(JobDetail job) {
        logger.info("Configuring trigger to fire every {} seconds", triggerInterval);
        return newTrigger()
        		.forJob(job)
        		.withIdentity(TriggerKey.triggerKey("Qrtz_Trigger"))
        		.withDescription("Sample trigger")
        		.withSchedule(simpleSchedule()
        				.withIntervalInSeconds(triggerInterval)
        				.repeatForever())
        		.build();
    }
    
    @Bean
    public JobDetail jobDetail() {
        return newJob()
        		.ofType(FilePopulatorJob.class)
        		.storeDurably()
        		.withIdentity(JobKey.jobKey("Qrtz_Job_Detail"))
        		.withDescription("Invoke Sample Job service...")
        		.build();
    }
}
