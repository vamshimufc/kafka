using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace Xplatform.Kafka
{
    public interface IKafkaConsumerHelper
    {
        void SendToDeadLetterQueue(ConsumeResult<Ignore, string> consumeResult);
    }
}
