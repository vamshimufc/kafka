using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Xplatform.Kafka
{
    public interface IKafkaProcessor<in T>
    {
        Task<CancellationToken> ProcessMessage(T t, ConsumeResult<Ignore, string> MessageDetails, CancellationToken Token);

        /// <summary>
        /// This method will trigger of kafka server faced issue while reading the message
        /// </summary>
        /// <param name="Message">Kafka message from subscribed topic</param>
        /// <param name="Reason">Error resaon emitted by kafka</param>
        /// <param name="LogLevel">LogLevel emitted by kafka</param>
        /// <returns></returns>
        String UnProcessedMessage(String Message, String Reason,SyslogLevel LogLevel);
    }
}
