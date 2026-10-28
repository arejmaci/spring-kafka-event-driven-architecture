package com.benja.course.springcloud.kafka.command.model;

public record Command<T>(CommandType type, Long id, T body) {
}
