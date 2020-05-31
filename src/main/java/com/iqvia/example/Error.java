package com.iqvia.example;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface Error {
	
	String OUTPUT = "myerror";
	
	@Output(Error.OUTPUT)
	  MessageChannel output();

}
