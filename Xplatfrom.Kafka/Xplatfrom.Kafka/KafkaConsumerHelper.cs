using Confluent.Kafka;
using System;

namespace Xplatform.Kafka
{
    public class KafkaConsumerHelper : IKafkaConsumerHelper
    {
        private readonly IKafkaConfig _config;

        public KafkaConsumerHelper(IKafkaConfig config)
        {
            _config = config;
        }

        /// <summary>
        /// Explicit implementation for dead letter queue for consumer(s) which requires messages to be stored in DLQ for any specific exceptions.
        /// </summary>
        /// <param name="consumeResult"></param>
        public void SendToDeadLetterQueue(ConsumeResult<Ignore, string> consumeResult)
        {
            if (_config.IsEnableConsumerDeadLetter())
            {
                KafkaProducer<String> kafkaProducer = new KafkaProducer<String>(_config, consumeResult.Message.Value);
                kafkaProducer.ProduceMessage(consumeResult.Message.Value, String.Concat(consumeResult.Topic, ".DeadLetter"));
            }
        }
    }
}
