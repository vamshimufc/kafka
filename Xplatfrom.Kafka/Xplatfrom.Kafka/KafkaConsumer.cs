using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Reflection;
using System.Threading;


namespace Xplatform.Kafka
{
    public class KafkaConsumer<T> : IKafkaConsumeMessageHelper
    {
        IKafkaConfig _config;

        T _desrializer;

        // private  K _serializer;
        IKafkaProcessor<T> _worker;

        ConsumeResult<Ignore, string> consumeResult;

        Offset CurrentOffset;

        Partition partition;

        String ErrorReason;

        CancellationToken cancellationToken;

        public KafkaConsumer(IKafkaConfig config, T deserializer, IKafkaProcessor<T> worker)
        {
            _config = config;
            _desrializer = deserializer;
            _worker = worker;
            consumeResult = new ConsumeResult<Ignore, string>();
            partition = new Partition();
            CurrentOffset = new Offset();
        }


        public void ConsumeMessage()
        {
            this.ConsumeMessage(_config.GetTopicName());
        }

        public void ConsumeMessage(string topicName)
        {


            String ErrorReason = String.Empty;

            var consumeBldr = new ConsumerBuilder<Ignore, string>(_config.ConsumerConfig());
            {
                //Added Error handler
                consumeBldr.SetErrorHandler(ConsumerErrorHandler);
                consumeBldr.SetLogHandler(LogHandler);
                var consumer = consumeBldr.Build();

                try
                {
                    consumer.Subscribe(topicName);
                    ReadMessages(consumer);
                }
                catch (Exception ex)
                {
                    // if this exception occured then 
                    ErrorReason = $"Error : { ex.Message } Type of { ex.GetType().Name } not handled and closing the consumer. { ex.InnerException?.Message } Source: { ex?.Source } .Stack :{ ex?.StackTrace}";
                    _worker.UnProcessedMessage("Empty Message Found", ErrorReason, SyslogLevel.Error);
                    consumer.Close();//TBD
                    consumer?.Dispose();
                }
            }
        }

        /// <summary>
        /// Log error emitted by Kafka
        /// </summary>
        /// <param name="arg1"></param>
        /// <param name="arg2"></param>
        private void LogHandler(IConsumer<Ignore, string> arg1, LogMessage arg2)
        {
            if (arg2.Level <= SyslogLevel.Notice)
                _worker.UnProcessedMessage("", arg2.Level + " LogHandler: Kafka Facility:" + arg2.Facility + ". Message: " + arg2.Message, arg2.Level);
        }

        /// <summary>
        /// Start reading the messages from kafka
        /// </summary>
        /// <param name="consumer"></param>
        /// <param name="cancellationToken"></param>
        private void ReadMessages(IConsumer<Ignore, string> consumer)
        {
            cancellationToken = new CancellationToken();

            while (!cancellationToken.IsCancellationRequested)
            {

                try
                {
                    consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(100));

                    //if no message found then look for next message again
                    if (consumeResult == null) continue;

                    if (!consumeResult.IsPartitionEOF)
                    {
                        var kafkamessage = JsonConvert.DeserializeObject(consumeResult.Value.ToString(), typeof(T));

                        cancellationToken = _worker.ProcessMessage((T)kafkamessage, consumeResult, cancellationToken).GetAwaiter().GetResult();

                        if (cancellationToken.IsCancellationRequested)
                            throw new OperationCanceledException("Cancellation token received.");

                        //Saving the Offset
                        consumer.StoreOffset(consumeResult);
                    }
                    else
                    {
                        _worker.UnProcessedMessage("No Message", "Info : End of Partition Reached." + (consumeResult.Value ?? String.Empty), SyslogLevel.Info);
                    }
                }
                // if this exception occured then ignore & move the offset ahead 
                catch (Exception Jex) when (Jex is JsonReaderException || Jex is JsonSerializationException)
                {
                    ErrorReason = string.Empty;
                    CurrentOffset = consumeResult.Offset;
                    partition = consumeResult.TopicPartition.Partition;
                    ErrorReason = "Warning : " + Jex.Message + ". Kafka offset moved ahead of offset " + CurrentOffset.Value.ToString() + " on partition " + partition.Value.ToString() + ". Message sent deadletter queue topic";
                    SendToDeadLetterQueue(consumeResult);
                    consumer.StoreOffset(consumeResult);
                    _worker.UnProcessedMessage(consumeResult.Value, ErrorReason, SyslogLevel.Warning);
                }
                // if this exception occured then do not move the offset as consumer request cancelation
                catch (OperationCanceledException Oex)
                {
                    ErrorReason = string.Empty;
                    CurrentOffset = consumeResult.Offset;
                    partition = consumeResult.TopicPartition.Partition;
                    ErrorReason = "Error : " + Oex.Message + " at Kafka offset position " + CurrentOffset.Value.ToString() + " of partition " + partition.Value;
                    _worker.UnProcessedMessage(consumeResult.Value, ErrorReason, SyslogLevel.Error);
                }
                catch (ConsumeException cex)
                {
                    ErrorReason = string.Empty;
                    ErrorReason = "Error : " + cex.Message;
                    _worker.UnProcessedMessage(consumeResult?.Value, ErrorReason, SyslogLevel.Error);
                }
            }
        }
        /// <summary>
        ///  This function will send the malform message to the respective topic dead letter queue if enabled.
        /// </summary>
        /// <param name="consumeResult"></param>
        private void SendToDeadLetterQueue(ConsumeResult<Ignore, string> consumeResult)
        {
            if (_config.IsEnableConsumerDeadLetter())
            {
                KafkaProducer<String> kafkaProducer = new KafkaProducer<String>(_config, consumeResult.Message.Value);
                kafkaProducer.ProduceMessage(consumeResult.Message.Value, String.Concat(consumeResult.Topic, ".DeadLetter"));
            }
        }

        /// <summary>
        /// To handle any consumer error
        /// </summary>
        /// <param name="arg1"></param>
        /// <param name="_error"></param>
        private void ConsumerErrorHandler(IConsumer<Ignore, string> arg1, Error _error)
        {
            String Msg = "No Message";
            ErrorReason = String.Empty;
            ErrorReason += "Warning : Consumer ErrorHandler.";
            ErrorReason += "Reason: " + _error.Reason;
            ErrorReason += "Details: " + JsonConvert.SerializeObject(_error);
            if (consumeResult != null)
                Msg = consumeResult.Value;

            _worker.UnProcessedMessage(Msg, ErrorReason, SyslogLevel.Warning);
        }


    }
}