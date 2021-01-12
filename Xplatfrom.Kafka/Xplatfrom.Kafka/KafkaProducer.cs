using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using System.Threading.Tasks;

namespace Xplatform.Kafka
{
    public class KafkaProducer<K> : IKafkaProduceMessageHelper<K>
    {
        private K _serializer;
        IKafkaConfig _config;
        ProducerBuilder<Null, string> _producerBldr;
        IProducer<Null, string> _producer;
        public KafkaProducer(IKafkaConfig config, K serializer)
        {
            _config = config;
            _serializer = serializer;
            _producerBldr = new ProducerBuilder<Null, string>(_config.ProducerConfig());
            _producerBldr.SetErrorHandler(ErrorHandler);
            _producer = _producerBldr.Build();
        }
        public void ProduceMessage(K message)
        {
            this.ProduceMessage(message, _config.GetProducerTopic());
        }
        public void ProduceMessage(K message, string topicName)
        {
            try
            {
                _producer.Produce(topicName, new Message<Null, string> { Value = JsonConvert.SerializeObject(message) });
            }
            catch (Exception ex)
            {
                System.Diagnostics.EventLog.WriteEntry("Xplatform.Kafka", ex.Message);
                if (ex.InnerException != null)
                    System.Diagnostics.EventLog.WriteEntry("Xplatform.Kafka", ex.InnerException.Message);
                throw ex;
            }
        }
        private void ErrorHandler(IProducer<Null, string> arg1, Error arg2)
        {
            if (arg2.IsFatal || arg2.IsError)
                throw new Exception("Kafka Producer ErrorHandler:" + arg2.Reason);
        }

        private void DeliveryHandler(DeliveryReport<Null, string> obj)
        {
            if (obj.Error.IsFatal || obj.Error.IsError)
            {
                System.Diagnostics.EventLog.WriteEntry("Xplatform.Kafka", "DeliveryHandler:" + obj.Error.Reason, System.Diagnostics.EventLogEntryType.Error);
                throw new Exception(obj.Error.Reason);
            }
        }

        public async Task ProduceMessageAsync(K message)
        {
            try
            {
                using (var producer = new ProducerBuilder<Null, string>(_config.ProducerConfig()).Build())
                {
                    // producer.Produce(topicName, new Message<Null, string> { Value =  },deliveryHandler);
                    var result = await producer.ProduceAsync(_config.GetProducerTopic(), new Message<Null, string> { Value = JsonConvert.SerializeObject(message) });
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}
