package it.eng.idsa.dataapp.web.rest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;

import javax.xml.datatype.DatatypeFactory;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.io.Files;

import de.fraunhofer.iais.eis.ArtifactRequestMessage;
import de.fraunhofer.iais.eis.ArtifactRequestMessageBuilder;
import de.fraunhofer.iais.eis.ArtifactResponseMessage;
import de.fraunhofer.iais.eis.Message;
import de.fraunhofer.iais.eis.ResponseMessage;
import de.fraunhofer.iais.eis.ids.jsonld.Serializer;
import it.eng.idsa.dataapp.service.OpenDataService;
import it.eng.idsa.dataapp.service.RecreateFileService;
import it.eng.idsa.dataapp.service.impl.MultiPartMessageServiceImpl;
import it.eng.idsa.multipart.domain.MultipartMessage;
import it.eng.idsa.multipart.processor.MultipartMessageProcessor;
import it.eng.idsa.streamer.WebSocketClientManager;
import it.eng.idsa.streamer.util.MultiPartMessageServiceUtil;
import it.eng.idsa.streamer.websocket.receiver.server.FileRecreatorBeanExecutor;

/**
 * @author Antonio Scatoloni
 */

@RestController
@EnableAutoConfiguration
@RequestMapping({ "/" })
public class FileSenderResource {
	private static final Logger logger = LogManager.getLogger(FileSenderResource.class);

	@Autowired
	MultiPartMessageServiceImpl multiPartMessageService;
	
	@Autowired
	RecreateFileService recreateFileService;
	
	@Autowired
	private OpenDataService opendataService;
	
	@Value("${application.dataLakeDirectory.destination}")
	private Path dataLakeDirectoryDestination;
	
	@Value("${application.opendata.ckan.datetimePattern:YYYYMMdd_HHmmss}")
	private String dateTimePattern;
	
	@GetMapping("/hello")
	@ResponseBody
	public String hello() {
		return "Hello";
	}
	
	@PostMapping("/requireandsavefile")
	@ResponseBody
	public String requireAndSaveFile(@RequestHeader("Forward-To-Internal") String forwardToInternal,
			@RequestHeader("Forward-To") String forwardTo, 
			@RequestBody String fileName) throws Exception {
		InputStream is = this.getClass().getClassLoader().getResourceAsStream("examples-multipart-messages/" + fileName);
		String message = IOUtils.toString(is, "UTF8");
		FileRecreatorBeanExecutor.getInstance().setForwardTo(forwardTo);
		String responseMessage = WebSocketClientManager.getMessageWebSocketSender()
				.sendMultipartMessageWebSocketOverHttps(message, forwardToInternal);

		String fileNameSaved = saveFileToDisk(responseMessage, message);

		String payload = "{​​\"message\":\"File '" + fileNameSaved + "' created successfully\"}";
		MultipartMessage multipartMessage = new MultipartMessage(
				new HashMap<>(), 
				new HashMap<>(),
				multiPartMessageService.getMessage(responseMessage),
				new HashMap<>(), 
				payload, 
				new HashMap<>(),
				null,
				null);
		return MultipartMessageProcessor.multipartMessagetoString(multipartMessage, false);
	}
	
	@PostMapping("/requirefile")
	@ResponseBody
	public String requireFile(@RequestHeader("Forward-To-Internal") String forwardToInternal,
			@RequestHeader("Forward-To") String forwardTo, 
			@RequestBody String fileName) throws Exception {
		InputStream is = this.getClass().getClassLoader().getResourceAsStream("examples-multipart-messages/" + fileName);
		String message = IOUtils.toString(is, "UTF8");
		FileRecreatorBeanExecutor.getInstance().setForwardTo(forwardTo);
		String responseMessage = WebSocketClientManager.getMessageWebSocketSender()
				.sendMultipartMessageWebSocketOverHttps(message, forwardToInternal);

		return responseMessage;
	}
	
	/**
	 * Should be used only for testing
	 * @param fileToUpload
	 * @return
	 * @throws Exception
	 */
	@GetMapping("/uploaddata")
	public ResponseEntity<String> uploadToCkan(@RequestParam String fileToUpload) throws Exception {
		logger.info("Endpoint for testing");
		try {
			logger.info("About to send file {}", fileToUpload);
			opendataService.uploadData(fileToUpload);
			logger.info("File {} uploaded to CKAN", fileToUpload);
		} catch (Exception e) {
			logger.error("Error while inserting data in CKAN", e);
			return new ResponseEntity<>("Failed to insert data in CKAN, check logs for more details", HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<>("Successfuly inserted data in CKAN", HttpStatus.OK);
	}
	
	
	@PostMapping("/artifactRequestMessage")
	@ResponseBody
	public String requestArtifact(@RequestHeader("Forward-To-Internal") String forwardToInternal,
			@RequestHeader("Forward-To") String forwardTo, @RequestParam String requestedArtifact,
			@Nullable @RequestBody String payload) throws Exception {
		URI requestedArtifactURI = URI
				.create("http://w3id.org/engrd/connector/artifact/" + requestedArtifact);
		Message artifactRequestMessage = new ArtifactRequestMessageBuilder()
				._issued_(DatatypeFactory.newInstance().newXMLGregorianCalendar(new GregorianCalendar()))
				._issuerConnector_(URI.create("http://w3id.org/engrd/connector"))._modelVersion_("4.0.0")
				._requestedArtifact_(requestedArtifactURI).build();
		Serializer serializer = new Serializer();
		String requestMessage = serializer.serialize(artifactRequestMessage);
		FileRecreatorBeanExecutor.getInstance().setForwardTo(forwardTo);
		String responseMessage = WebSocketClientManager.getMessageWebSocketSender()
				.sendMultipartMessageWebSocketOverHttps(requestMessage, payload, forwardToInternal);

		String fileNameSaved = saveFileToDisk(responseMessage, artifactRequestMessage);

		String payloadResponse = null;
		if(fileNameSaved != null) {
			String filePartName = dataLakeDirectoryDestination.resolve(fileNameSaved).toString();
			logger.info("About to send file {}", filePartName);
			opendataService.uploadData(filePartName);
			payloadResponse = "{​​\"message\":\"File '" + fileNameSaved + "' uploaded to CKAN successfully\"}";
		} else {
			payloadResponse = "{​​\"message\":\"File did not uploaded to CKAN\"}";
		}
		
		MultipartMessage multipartMessage = new MultipartMessage(
				new HashMap<>(), 
				new HashMap<>(),
				multiPartMessageService.getMessage(responseMessage),
				new HashMap<>(), 
				payloadResponse, 
				new HashMap<>(),
				null,
				null);
		return MultipartMessageProcessor.multipartMessagetoString(multipartMessage, false);
	}
	
	private String saveFileToDisk(String responseMessage, String requestMessage) throws IOException {
		Message requestMsg = multiPartMessageService.getMessage(requestMessage);
		Message responseMsg = multiPartMessageService.getMessage(responseMessage);
		
		String payload = MultiPartMessageServiceUtil.getPayload(responseMessage);

		String requestedArtifact = null;
		if (requestMsg instanceof ArtifactRequestMessage && responseMsg instanceof ResponseMessage) {
			requestedArtifact = ((ArtifactRequestMessage) requestMsg).getRequestedArtifact().getPath().split("/")[2];
			logger.info("About to save file " + requestedArtifact);
			recreateFileService.recreateTheFile(payload, new File(requestedArtifact));
			logger.info("File saved");
		} else {
			logger.info("Did not have ArtifactRequestMessage and ResponseMessage - nothing to save");
		}
		
		return requestedArtifact;
	}
	/**
	 * Save payload content to file on file system</br>
	 * If payload is null - skips creating file and returns null as file name
	 * @param responseMessage
	 * @param requestMessage
	 * @return
	 * @throws IOException
	 */
	private String saveFileToDisk(String responseMessage, Message requestMessage) throws IOException {
		Message responseMsg = multiPartMessageService.getMessage(responseMessage);

		String requestedArtifact = null;
		if (requestMessage instanceof ArtifactRequestMessage && responseMsg instanceof ArtifactResponseMessage) {
			String payload = MultiPartMessageServiceUtil.getPayload(responseMessage);
			if(payload != null) {
				String reqArtifact = ((ArtifactRequestMessage) requestMessage).getRequestedArtifact().getPath();
				requestedArtifact = reqArtifact.substring(reqArtifact.lastIndexOf('/') + 1);
				logger.info("About to save file " + requestedArtifact);
				String finalFileName = addTimestampToFileName(requestedArtifact);
				recreateFileService.recreateTheFile(payload, dataLakeDirectoryDestination.resolve(finalFileName).toFile());
				requestedArtifact = finalFileName;
				logger.info("File saved");
			} else {
				logger.info("Artifact response Message received, but no payload");
				requestedArtifact = null;
			}
		} else {
			logger.info("Did not have ArtifactRequestMessage and ResponseMessage - nothing to save");
			requestedArtifact = null;
		}
		
		return requestedArtifact;
	}

	private String addTimestampToFileName(String requestedArtifact) {
		DateFormat formatter = new SimpleDateFormat(dateTimePattern);
		String formattedDate = formatter.format(new Date());
		String extension = Files.getFileExtension(requestedArtifact);
		String fileName = Files.getNameWithoutExtension(requestedArtifact);
		String finalFileName = fileName + "_" + formattedDate + "." + extension;
		return finalFileName;
	}

}
