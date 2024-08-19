package com.acc.kafkastreamsdemo.producer;

import com.acc.kafkastreamsdemo.event.RequestPlan;
import com.acc.kafkastreamsdemo.topology.MessageTopology;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class RequestPlanProducer {
    private final KafkaTemplate<Integer,String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public String topic= MessageTopology.GREETINGS_TOPIC;

    public RequestPlanProducer(ObjectMapper objectMapper, KafkaTemplate<Integer, String> kafkaTemplate) {
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }


    public CompletableFuture<SendResult<Integer,String>> sendEvent(RequestPlan requestPlan) throws JsonProcessingException {
        var key=requestPlan.planId();
        var value=objectMapper.writeValueAsString(requestPlan);

        var completableFuture=kafkaTemplate.send(topic,key,value);
        return completableFuture
                .whenComplete((sendResult,throwable)->{
                    if (throwable!=null){
                        log.error("hata");
                    }else{
                        handleSuccess(key,value,sendResult);
                    }
                });
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message sent successfully :" + key);
    }
}
