package com.acc.kafkastreamsdemo.producer;

import com.acc.kafkastreamsdemo.event.RequestPlan;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.stereotype.Component;

@Component
public class RequestPlanService {
    private final RequestPlanProducer requestPlanProducer;

    public RequestPlanService(RequestPlanProducer requestPlanProducer) {
        this.requestPlanProducer = requestPlanProducer;
    }

    public void SendEvent(RequestPlan requestPlan) throws JsonProcessingException {
        requestPlanProducer.sendEvent(requestPlan);
    }
}
