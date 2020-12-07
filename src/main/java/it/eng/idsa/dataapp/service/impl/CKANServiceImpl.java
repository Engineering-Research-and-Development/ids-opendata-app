package it.eng.idsa.dataapp.service.impl;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.commons.io.FilenameUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.FormBodyPart;
import org.apache.http.entity.mime.FormBodyPartBuilder;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

import it.eng.idsa.dataapp.service.CKANService;

@Service
public class CKANServiceImpl implements CKANService {
	
	private static final Logger logger = LogManager.getLogger(CKANService.class);

	@Value("${application.ckan.url}")
	private String ckanURL;
	
	@Value("${application.ckan.token}")
	private String token;
	
	@Value("${application.ckan.packageId}")
	private String packageId;
	
	@Value("${application.dataLakeDirectory.destination}")
	private Path dataLakeDirectory;
	
	@Override
	public void sendFileToCkan(String fileName) throws IOException {
		HttpPost httpPost = new HttpPost(ckanURL);
		httpPost.addHeader("Authorization", token);
		
		MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create().setMode(HttpMultipartMode.STRICT);
		
		HttpClient httpclient = HttpClients.custom().build();
		
		ContentBody packageIdBody = new StringBody(packageId, ContentType.DEFAULT_TEXT);
		FormBodyPart packagePayloadPart = FormBodyPartBuilder.create("package_id", packageIdBody).build();
		multipartEntityBuilder.addPart(packagePayloadPart);
		
		ContentBody nameBody = new StringBody(FilenameUtils.getName(fileName), ContentType.DEFAULT_TEXT);
		FormBodyPart namePayloadPart = FormBodyPartBuilder.create("name", nameBody).build();
		multipartEntityBuilder.addPart(namePayloadPart);
		
		HttpEntity httpEntity = multipartEntityBuilder
			    .addBinaryBody("upload", 
			    		dataLakeDirectory.resolve(fileName).toFile(),
			    		ContentType.create("text/csv"), fileName)
			    .build();
				
		httpPost.setEntity(httpEntity);

		HttpResponse response = null;
		logger.info("Sending file part to CKAN...");
		try {
			response = httpclient.execute(httpPost);
		} catch (IOException e) {
			logger.error("Error while making call to CKAN using URL {}\n {}", ckanURL, e);
			throw new IOException(e);
		}
		logger.info("Received response from CKAN {}", response);
		if(response.getStatusLine().getStatusCode() >= 300) {
			throw new RestClientException("Error while calling CKAN"); 
		} 
	}

}