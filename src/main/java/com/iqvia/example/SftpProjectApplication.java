package com.iqvia.example;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import com.google.gson.JsonObject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.file.FileNameGenerator;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.integration.file.filters.ChainFileListFilter;
import org.springframework.integration.file.filters.FileSystemPersistentAcceptOnceFileListFilter;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.integration.jdbc.metadata.JdbcMetadataStore;
import org.springframework.integration.metadata.MetadataStore;
import org.springframework.integration.metadata.PropertiesPersistingMetadataStore;
import org.springframework.integration.sftp.dsl.Sftp;
import org.springframework.integration.sftp.dsl.SftpMessageHandlerSpec;
import org.springframework.integration.sftp.filters.SftpPersistentAcceptOnceFileListFilter;
import org.springframework.integration.sftp.filters.SftpSimplePatternFileListFilter;
import org.springframework.integration.sftp.inbound.SftpInboundFileSynchronizer;
import org.springframework.integration.sftp.inbound.SftpInboundFileSynchronizingMessageSource;
import org.springframework.integration.sftp.outbound.SftpMessageHandler;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.MessagingException;
import org.springframework.util.FileCopyUtils;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import org.springframework.expression.Expression;

@SpringBootApplication
@EnableBinding(value = { Source.class, Error.class })
public class SftpProjectApplication {

	@Autowired
	private Error error;

	public static void main(String[] args) {
		new SpringApplicationBuilder(SftpProjectApplication.class).web(WebApplicationType.NONE).run(args);
	}

	@Bean
	public SessionFactory<LsEntry> sftpSessionFactory() {
		DefaultSftpSessionFactory factory = new DefaultSftpSessionFactory(true);
		factory.setHost("10.162.176.104");
		factory.setPort(22);
		factory.setUser("sanad.uat");
		factory.setPassword("suat201607");
		factory.setAllowUnknownKeys(true);
		return new CachingSessionFactory<LsEntry>(factory);
	}

	/*
	 * Filter set up here will act while fetching file from remote server one filter
	 * here will allow only *.txt files and other filter will not allow files to be
	 * fetched again b/w application restarts, this requires a persisting
	 * metadatastore.
	 */
	@Bean
	public SftpInboundFileSynchronizer sftpInboundFileSynchronizer() {

		SftpInboundFileSynchronizer fileSynchronizer = new SftpInboundFileSynchronizer(sftpSessionFactory());
		fileSynchronizer.setDeleteRemoteFiles(false);
		fileSynchronizer.setRemoteDirectory("/KFSH/");
		fileSynchronizer.setTemporaryFileSuffix(".tmp");
		ChainFileListFilter<LsEntry> filterChain = new ChainFileListFilter<>();
		filterChain.addFilter(new SftpSimplePatternFileListFilter("*.txt"));
		filterChain.addFilter(new SftpPersistentAcceptOnceFileListFilter(metadataStore(), "sftpSource/"));
		fileSynchronizer.setFilter(filterChain);
		fileSynchronizer.setPreserveTimestamp(true);
		fileSynchronizer.setDeleteRemoteFiles(true);
//	        fileSynchronizer.setRemoteFileMetadataStore(metadataStore());
		return fileSynchronizer;
	}

	/*
	 * here a local filter is there which will act after files from remote server
	 * have come to your machine And this will act when file from file system are
	 * picked up and being processed.
	 */
	@Bean
	@InboundChannelAdapter(channel = "sftpChannel", poller = @Poller(fixedDelay = "1"))
	public MessageSource<File> sftpMessageSource() {
		SftpInboundFileSynchronizingMessageSource source = new SftpInboundFileSynchronizingMessageSource(
				sftpInboundFileSynchronizer());
		source.setLocalDirectory(new File("C:\\Users\\pradeep.bhati\\error"));
		source.setAutoCreateLocalDirectory(true);
		FileSystemPersistentAcceptOnceFileListFilter localfilter = new FileSystemPersistentAcceptOnceFileListFilter(
				localmetadataStore(), "rollback:");
		source.setLocalFilter(localfilter);
		/*
		 * this local filter will act only in one session, it will not persist b/w
		 * application restarts
		 */
//	        source.setLocalFilter(new AcceptOnceFileListFilter<File>());
		source.setMaxFetchSize(-1);
		return source;
	}

	/* A transformer to transform file to String or Byte[] and put on kafka */
	@Transformer(inputChannel = "sftpChannel", outputChannel = Source.OUTPUT)
	Message<String> stringTransformer(Message<?> message) throws IOException {
		File file = (File) message.getPayload();
////	    	 return MessageBuilder.withPayload(file).copyHeaders(message.getHeaders())
////					 .setHeader("dir", "mgm").build();
//	    	Path path = Paths.get(file.toString());
//	        byte[] data = null;
//			try {
//				data = Files.readAllBytes(path);
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				System.out.println("got exception while converting to byte[]");
//				e.printStackTrace();
//			}
//	        return MessageBuilder.withPayload(data)
//	                .build();
//	    }
		Person person = null;
		Reader reader;
//			try {
		reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
		String filestring = FileCopyUtils.copyToString(reader);
		person = new ObjectMapper().readValue(filestring, Person.class);
		Map<String, Object> hm = new HashMap<String, Object>();
		String dirname = person.getResourcetype();
		hm.put("dir", dirname);
		JsonObject js = new JsonObject();
		js.addProperty("dir", dirname);
//				message.getHeaders().put("dir", person.getResourcetype());
//				return message;
		return MessageBuilder.withPayload(filestring).build();

//			} 
//			catch (Exception e) {
//				// TODO Auto-generated catch block
//				System.out.println("exception occured");
//				System.out.println(e.getMessage());
//				e.printStackTrace();
//				return MessageBuilder.withPayload("exception occured")
//		                .build();
//			}
	}

	@Bean
	public PropertiesPersistingMetadataStore metadataStore() {
		PropertiesPersistingMetadataStore metadataStore = new PropertiesPersistingMetadataStore();
		metadataStore.setBaseDirectory("C:\\Users\\pradeep.bhati\\tmp\\foo");
		return metadataStore;
	}

	@Bean
	public PropertiesPersistingMetadataStore localmetadataStore() {
		PropertiesPersistingMetadataStore metadataStore = new PropertiesPersistingMetadataStore();
		metadataStore.setBaseDirectory("C:\\Users\\pradeep.bhati\\tmp\\localfoo");
		return metadataStore;
	}

	/*
	 * A transformer to catch exceptions from errorChannel and put on a kafka topic
	 */
	@Transformer(inputChannel = "errorChannel", outputChannel = Error.OUTPUT)
	String transformError(Message<MessagingException> message) {
		return message.toString();
	}

	/* this code is for setting up a mysql based metadatastore */

//		    @Bean
//		    public MetadataStore metadataStore(DataSource dataSource) {
//		        return new JdbcMetadataStore(dataSource);
//		    }
//		    
//		    @Bean
//		    public DataSource dataSource()
//		    {
//		    	DataSourceBuilder<?> dataSourceBuilder = DataSourceBuilder.create();
//		    	dataSourceBuilder.driverClassName("com.mysql.jdbc.Driver");
//		    	dataSourceBuilder.url("jdbc:mysql://localhost:3306/trinity");
//		    	dataSourceBuilder.username("root");
//		    	dataSourceBuilder.password("root");
//		    	return dataSourceBuilder.build();
//		    }

	/*
	 * A outbound adapter was written here because wanted to check remote directory
	 * expression to create remote directory dynamically based on header and payload
	 * expression. This code was added for testing purpose and is now present in
	 * sftp sink. In case both remote-directory and remote-directory expression are
	 * given, remote directory expression will take precedence. To use this add
	 * sftpout as output channel in transformer instead of Source.OUTPUT
	 */

//		    	    @Bean
//		    		public IntegrationFlow ftpInboundFlow() {
//		    			SftpMessageHandlerSpec handlerSpec =
//		    					Sftp.outboundAdapter(new SftpRemoteFileTemplate(sftpSessionFactory()), FileExistsMode.REPLACE)
//		    							.remoteDirectory("/headers.connection/")
////		    							.remoteFileSeparator(properties.getRemoteFileSeparator())
////		    							.fileNameExpression("headers.timestamp")
////		    							.fileNameGenerator(generate())		
//		    							.remoteDirectoryExpression("headers.dir")
//		    							.autoCreateDirectory(true)
//		    							.temporaryFileSuffix(".tmp");			
//		    				
//		    				return IntegrationFlows.from("sftpout")
//		    						.handle(handlerSpec)
//		    						.get();
//		    				
//		    			}

	/*
	 * FileNameGenerator is used to generate file name dynamically using header and
	 * payload expression. It was being used with SftpOutboundAdapter.
	 */
	@Bean
	public FileNameGenerator generate() {
		FileNameGenerator filenamegen = new FileNameGenerator() {

			@Override
			public String generateFileName(Message<?> message) {
				// TODO Auto-generated method stub
				String str = message.getHeaders().getTimestamp().toString();
				return str;
			}

		};

		return filenamegen;
	}

//	    @Bean
//	    @ServiceActivator(inputChannel = "sftpChannel")
//	    public MessageHandler handler() {
//	        return new MessageHandler() {
//
//	            @Override
//	            public void handleMessage(Message<?> message) throws MessagingException {
//	                System.out.println(message.getPayload());
//	            }
//
//	        };
//	    }

}
