using System;
using System.Collections.Generic;
using System.Text;

 namespace Xplatform.Kafka
{
   public interface IKafkaConsumeMessageHelper
    {
        void ConsumeMessage();
        void ConsumeMessage(string topicName);
    }
}
