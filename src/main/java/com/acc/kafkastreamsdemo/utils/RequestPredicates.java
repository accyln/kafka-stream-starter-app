package com.acc.kafkastreamsdemo.utils;

import com.acc.kafkastreamsdemo.domain.Environment;
import com.acc.kafkastreamsdemo.domain.OrderType;
import com.acc.kafkastreamsdemo.event.RequestPlan;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.EnumMap;
import java.util.Map;

public class RequestPredicates {

    public static <K> Predicate<K, RequestPlan> environmentMatches(Environment env) {
        return (key, request) -> request.environment() == env;
    }


    public static <K> Predicate<K, RequestPlan> typeMatches(OrderType type) {
        return (key, request) -> request.orderType() == type;
    }


    public static <K> Predicate<K, RequestPlan> environmentAndTypeMatches(Environment env, OrderType type) {
        return (key, request) -> request.environment() == env && request.orderType() == type;
    }

//    public static getAllPredicatesForEnvironmentAndTypes(){
//
//        Map<Environment, Map<OrderType, KStream<String, RequestPlan>>> branches = new EnumMap<>(Environment.class);
//
//        for (Environment env : Environment.values()) {
//            Map<OrderType, KStream<String, RequestPlan>> typeBranches = new EnumMap<>(OrderType.class);
//            for (OrderType type : OrderType.values()) {
//                Predicate<String, RequestPlan> predicate = RequestPredicates.environmentAndTypeMatches(env, type);
//                typeBranches.put(type, requestStream.filter(predicate));
//            }
//            branches.put(env, typeBranches);
//        }
//    }

}
