using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xplatform.Kafka;

namespace XPlatform.Kafka.UnitTest.Models
{
    public class KProcessor : IKafkaProcessor<Rootobject>
    {
        public string UnProcessedMessage(string Message, string Reason,SyslogLevel log)
        {
            return "";
        }

        async Task<CancellationToken> IKafkaProcessor<Rootobject>.ProcessMessage(Rootobject t, ConsumeResult<Ignore, string> MessageDetails, CancellationToken Token)
        {
            CancellationTokenSource source = new CancellationTokenSource();
             Token = source.Token;
            if(t.FileName.Contains("cancel"))
            source.Cancel();
            return Token;
        }
    }
}
