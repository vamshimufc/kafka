using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Xplatform.Kafka
{
   public interface IKafkaProduceMessageHelper<K>
    {
        void ProduceMessage(K message,string topicName);
        void ProduceMessage(K message);

         Task ProduceMessageAsync(K message);
    }
}
