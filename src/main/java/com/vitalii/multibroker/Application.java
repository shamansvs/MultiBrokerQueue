package com.vitalii.multibroker;

import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.broker.inmemory.InMemoryMessageBroker;
import com.vitalii.multibroker.config.AppConfig;
import com.vitalii.multibroker.consumer.ConsumerRunner;
import com.vitalii.multibroker.csv.InvalidCsvWriter;
import com.vitalii.multibroker.csv.ValidCsvWriter;
import com.vitalii.multibroker.generator.MessageGenerator;
import com.vitalii.multibroker.processing.MessageProcessor;
import com.vitalii.multibroker.producer.MessageProducer;
import com.vitalii.multibroker.producer.ProducerRunner;
import com.vitalii.multibroker.validation.MessageValidator;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Application {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        AppConfig config = AppConfig.load();
        MessageBroker broker = new InMemoryMessageBroker();
        long totalStart = System.nanoTime();

        try (ValidatorFactory factory = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory();
             ValidCsvWriter validWriter = new ValidCsvWriter(config.validCsvPath());
             InvalidCsvWriter invalidWriter = new InvalidCsvWriter(config.invalidCsvPath())) {

            MessageValidator validator = new MessageValidator(factory.getValidator());
            MessageProcessor processor = new MessageProcessor(validator, validWriter, invalidWriter);
            MessageGenerator generator = new MessageGenerator();
            MessageProducer producer = new MessageProducer(generator, broker, config.queueName());
            ProducerRunner producerRunner = new ProducerRunner(producer, broker, config.queueName(),
                    config.messagesCount(), config.consumersCount());

            try (ConsumerRunner consumerRunner = new ConsumerRunner(broker, processor,
                    config.queueName(), config.consumersCount())) {
                consumerRunner.start();
                producerRunner.run();
                consumerRunner.awaitCompletion();
            }

            long totalMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - totalStart);
            LOGGER.info("Total time: {} ms", totalMillis);
        }
    }
}
