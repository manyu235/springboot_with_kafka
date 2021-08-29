package com.kafka.test.kafkatest.controller;

import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.kafka.test.kafkatest.producerconfig.KafkaProducerConfig;

@RestController
@RequestMapping(value = "/demo/v1/")
public class TestController 
{
	
	@Autowired
	KafkaProducerConfig producer;
	
	@RequestMapping(value = "write")
	public String postMessage() throws InterruptedException, ExecutionException
	{
		producer= new KafkaProducerConfig();
		KafkaTemplate<String, String> kafkaTemplate=producer.kafkaTemplate();
		ListenableFuture<SendResult<String, String>> sendResultMsg=kafkaTemplate.send("test-topic-1","Publishing message from TestController");
		RecordMetadata metaData = sendResultMsg.get().getRecordMetadata();
		System.out.println("Message sent to : topic <"+metaData.topic() + ">...offset...<"+ metaData.offset() +">...partition...<" + metaData.partition()+">");
		return "Publishing message from TestController";
		
	}
	
	@KafkaListener(topics = "test-topic-1", groupId = "test", containerFactory = "kafkaListenerContainerFactory")
	  public void receiveMessage(@Payload String message, @Headers MessageHeaders headers) 
	   {
	        System.out.println("Message received: "+message +"\n" +headers.toString());
	   }

}