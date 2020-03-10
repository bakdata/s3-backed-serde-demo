package com.bakdata.kafka.util;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class TopicInfo {
    int partitions;
    short replicationFactor;
}
