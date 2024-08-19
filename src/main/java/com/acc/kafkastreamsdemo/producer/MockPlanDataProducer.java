package com.acc.kafkastreamsdemo.producer;

import com.acc.kafkastreamsdemo.domain.Environment;
import com.acc.kafkastreamsdemo.domain.OrderType;
import com.acc.kafkastreamsdemo.event.RequestPlan;
import com.acc.kafkastreamsdemo.topology.RequestPlanTopology;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.time.LocalDateTime;
import java.util.List;

import static com.acc.kafkastreamsdemo.producer.ProducerUtil.publishMessageSync;

@Slf4j
public class MockPlanDataProducer {

    public static void main(String[] args) throws InterruptedException {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        publishOrders(objectMapper, buildPlans());
        //publishBulkOrders(objectMapper);

    }

    private static void publishOrders(ObjectMapper objectMapper, List<RequestPlan> plans) {

        plans
                .forEach(plan -> {
                    try {
                        var ordersJSON = objectMapper.writeValueAsString(plan);
                        var recordMetaData = publishMessageSync("example-input-topic", plan.planId()+"", ordersJSON);
                        log.info("Published the order message : {} ", recordMetaData);
                    } catch (JsonProcessingException e) {
                        log.error("JsonProcessingException : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                    catch (Exception e) {
                        log.error("Exception : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                });
    }

    private static List<RequestPlan> buildPlans() {


        var plan1 = new RequestPlan(12345, OrderType.DNS,
                Environment.DEV,
                LocalDateTime.now()
        );

        var plan2 = new RequestPlan(54321, OrderType.LOADBALANCER,
                Environment.SENLIVE,
                LocalDateTime.now()
        );


        return List.of(
                plan1,
                plan2
        );
    }
}
