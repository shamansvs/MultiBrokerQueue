package com.vitalii.multibroker.serialization;

import com.vitalii.multibroker.model.QueueMessage;

public interface QueueMessageSerializer {
    byte[] serialize(QueueMessage message);

    QueueMessage deserialize(byte[] bytes);
}
