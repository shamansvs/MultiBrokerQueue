package com.vitalii.multibroker.model;

public sealed interface QueueMessage permits PojoMessage, PoisonPill {
}
