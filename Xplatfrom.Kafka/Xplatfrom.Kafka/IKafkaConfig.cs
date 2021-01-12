using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;

namespace Xplatform.Kafka
{
   public  interface IKafkaConfig
    {
        ConsumerConfig ConsumerConfig();
        ProducerConfig ProducerConfig();

        string GetTopicName();
        string GetProducerTopic();

        int GetTimeOut();

        /// <summary>
        /// Enabling this property will save malformed message repective the deadletter topic of consumer
        /// </summary>
        /// <returns></returns>
        bool IsEnableConsumerDeadLetter();
        
    }
}
