package org.folio.kafka;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class SubscriptionDefinition {
  private final String eventType;
  private final String subscriptionPattern;
}
