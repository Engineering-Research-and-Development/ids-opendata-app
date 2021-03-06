package it.eng.idsa.dataapp.web.rest;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import de.fraunhofer.iais.eis.ArtifactRequestMessage;
import de.fraunhofer.iais.eis.Message;
import it.eng.idsa.dataapp.exception.EmptyFileException;
import it.eng.idsa.dataapp.exception.ReadFileLockedException;
import it.eng.idsa.dataapp.service.FileReaderService;
import it.eng.idsa.dataapp.service.MultiPartMessageService;
import it.eng.idsa.multipart.builder.MultipartMessageBuilder;
import it.eng.idsa.multipart.domain.MultipartMessage;
import it.eng.idsa.multipart.processor.MultipartMessageProcessor;
import it.eng.idsa.streamer.WebSocketServerManager;

/**
 * @author Antonio Scatoloni
 */

public class IncomingDataAppResourceOverWs implements PropertyChangeListener {

	private static final Logger logger = LogManager.getLogger(IncomingDataAppResourceOverWs.class);

	@Autowired
	private MultiPartMessageService multiPartMessageService;
	
	@Autowired
	private FileReaderService fileReaderService;
	
	@Override
	public void propertyChange(PropertyChangeEvent evt) {
		String requestMessageMultipart = (String) evt.getNewValue();
		Message requestMessage = multiPartMessageService.getMessage(requestMessageMultipart);
		String requestHeader = multiPartMessageService.getHeader(requestMessageMultipart);
		String requestedArtifact = null;
		String responseMessageString = null;
		if (requestMessage instanceof ArtifactRequestMessage) {
			String reqArtifact = ((ArtifactRequestMessage) requestMessage).getRequestedArtifact().getPath();
			// get resource from URI http://w3id.org/engrd/connector/artifact/ + requestedArtifact
			requestedArtifact = reqArtifact.substring(reqArtifact.lastIndexOf('/') + 1);
			logger.info("About to get file from " + requestedArtifact);
			String responsePayload = null;
			try {
				responsePayload = fileReaderService.readRequestedArtifact(requestMessage, requestedArtifact);
			} catch (ReadFileLockedException | EmptyFileException | IOException e) {
				logger.error("Error while reading resource from disk - creating rejection message", e);
				Message rejectionMessage = multiPartMessageService.createRejectionCommunicationLocalIssues(requestMessage);
				MultipartMessage responseMessageRejection = new MultipartMessageBuilder()
						.withHeaderContent(rejectionMessage)
						.withPayloadContent(null)
						.build();
				responseMessageString = MultipartMessageProcessor.multipartMessagetoString(responseMessageRejection, false);
			}
			String responseMessage = multiPartMessageService.getResponseHeader(requestMessage);
			// prepare multipart message.
			MultipartMessage responseMessageMultipart = new MultipartMessageBuilder().withHeaderContent(responseMessage)
					.withPayloadContent(responsePayload).build();
			responseMessageString = MultipartMessageProcessor.multipartMessagetoString(responseMessageMultipart, false);
		} else {
			responseMessageString = createDummyResponse(requestHeader);
		}
		WebSocketServerManager.getMessageWebSocketResponse().sendResponse(responseMessageString);
	}

	private String createDummyResponse(String resquestMessage) {
		String responseMessageString = null;
		try {
			String responsePayload = createResponsePayload();
			// prepare multipart message.
			MultipartMessage responseMessage = new MultipartMessageBuilder().withHeaderContent(resquestMessage)
					.withPayloadContent(responsePayload).build();
			responseMessageString = MultipartMessageProcessor.multipartMessagetoString(responseMessage, false);

		} catch (Exception e) {
			logger.error("Error while creating dummy response", e);
			Message rejectionMessage = multiPartMessageService.createRejectionMessageLocalIssues(multiPartMessageService.getMessage(resquestMessage));
			MultipartMessage responseMessageRejection = new MultipartMessageBuilder().withHeaderContent(rejectionMessage)
					.withPayloadContent(null).build();
			responseMessageString = MultipartMessageProcessor.multipartMessagetoString(responseMessageRejection, false);
		}
		return responseMessageString;
	}

	private String createResponsePayload() {
		// Put check sum in the payload
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		String formattedDate = dateFormat.format(date);

		Map<String, String> jsonObject = new HashMap<>();
		jsonObject.put("firstName", "John");
		jsonObject.put("lastName", "Doe");
		jsonObject.put("dateOfBirth", formattedDate);
		jsonObject.put("address", "591  Franklin Street, Pennsylvania");
		jsonObject.put("checksum", "ABC123 " + formattedDate);
		Gson gson = new GsonBuilder().create();
		return gson.toJson(jsonObject);
	}
}
