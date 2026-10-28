package com.benja.course.springcloud.kafka.command.model;

public record Reply<T>(ReplyStatus status, String message, T body) {
}
