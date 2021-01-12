using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using System;
using System.Net;

namespace Xplatform.Kafka
{
    public class KafkaConfig : IKafkaConfig
    {
        //TO DO : Move all the config to a JSON file 
        IConfiguration _configuration;
        public KafkaConfig(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        /// <summary>
        /// read only property 
        /// </summary>
        public string GetTopicName()
        {
            return _configuration["Kafka:TopicName"];
        }

        #region Producers Configuration

        public ProducerConfig ProducerConfig()
        {
            ProducerConfig _ProducerConfig = new ProducerConfig
            {
                MaxInFlight = 3,
                MessageSendMaxRetries = 3,
                Acks = Acks.All,
                BootstrapServers = GetProducerKafkaNodes(),
                ClientId = Dns.GetHostName()
            };

            // Only if Producer kafka cluster secured
            if (IsProducerSecured())
            {
                _ProducerConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
                _ProducerConfig.SaslMechanism = GetProducerSaslMechanism();
                _ProducerConfig.SaslUsername = GetProducerSaslUsername();
                _ProducerConfig.SaslPassword = GetProducerSaslPassword();
                _ProducerConfig.SslCaLocation = GetProducerSaslCaLocation();
                _ProducerConfig.EnableSslCertificateVerification = EnableSSLVerificationForProducer();
            }

            return _ProducerConfig;
        }

        public string GetProducerTopic()
        {
            return _configuration["Kafka:ProducerTopicName"];
        }

        /// <summary>
        /// Pick the default kafkanodes if producer nodes are not explictly mentioned in clients config
        /// </summary>
        public String GetProducerKafkaNodes()
        {
            return _configuration["Kafka:KafkaProducerNodes"] ?? _configuration["Kafka:KafkaNodes"];
        }

        /// <summary>
        /// Checks if secure connection is mentioned in client config
        /// </summary>
        /// <returns></returns>
        public bool IsProducerSecured()
        {
            return Convert.ToBoolean((_configuration["Kafka:IsProducerSecured"] ?? "false"));
        }

        /// <summary>
        /// Read SaslMechanism config key. If the value is null/empty it will default to Plain
        /// </summary>
        /// <returns></returns>
        public SaslMechanism GetProducerSaslMechanism()
        {
            return GetSaslMechanism(_configuration["Kafka:ProducerSaslMechanism"] ?? _configuration["Kafka:SaslMechanism"]);
        }

        /// <summary>
        /// Flag to turn on/off EnableSSLCertificateVerification
        /// </summary>
        /// <returns></returns>
        public bool EnableSSLVerificationForProducer()
        {
            return EnableSSLVerification(_configuration["Kafka:EnableSSLVerificationForProducer"] ?? _configuration["Kafka:EnableSSLVerification"]);
        }

        /// <summary>
        /// Gets SaslUsername for producer from configuration
        /// </summary>
        /// <returns></returns>
        public string GetProducerSaslUsername()
        {
            return _configuration["Kafka:ProducerSaslUsername"] ?? _configuration["Kafka:SaslUsername"];
        }

        /// <summary>
        /// Gets SaslPassword for producer from configuration
        /// </summary>
        /// <returns></returns>
        private string GetProducerSaslPassword()
        {
            return _configuration["Kafka:ProducerSaslPassword"] ?? _configuration["Kafka:SaslPassword"];
        }

        /// <summary>
        /// Gets SslCaLocation cert location for producer to connect.
        /// </summary>
        /// <returns></returns>
        private string GetProducerSaslCaLocation()
        {
            return (_configuration["Kafka:ProducerSslCaLocation"] ?? _configuration["Kafka:SslCaLocation"]) ?? string.Empty;
        }

        #endregion

        #region Consumer Configuration

        public ConsumerConfig ConsumerConfig()
        {

            ConsumerConfig _ConsumerConfig = new ConsumerConfig
            {
                BootstrapServers = GetConsumerKafkaNodes(),
                GroupId = _configuration["Kafka:ConsumerGroup"],
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = (AutoOffsetReset?) Enum.Parse(typeof(AutoOffsetReset), _configuration["Kafka:AutoOffsetReset"])
                //  AutoOffsetReset = Enum.Parse<AutoOffsetReset>(_configuration["Kafka:AutoOffsetReset"]),
            };

            // Only if consumer kafka cluster secured
            if (IsConsumerSecured())
            {
                _ConsumerConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
                _ConsumerConfig.SocketTimeoutMs = 60000; //this corresponds to the Consumer config `request.timeout.ms`
                _ConsumerConfig.SessionTimeoutMs = 30000;
                _ConsumerConfig.SaslMechanism = GetConsumerSaslMechanism();
                _ConsumerConfig.SaslUsername = GetConsumerSaslUsername();
                _ConsumerConfig.SaslPassword = GetConsumerSaslPassword();
                _ConsumerConfig.SslCaLocation = GetConsumerSaslCaLocation();
                _ConsumerConfig.EnableSslCertificateVerification = EnableSSLVerificationForConsumer();
                _ConsumerConfig.BrokerVersionFallback = "1.0.0";
            }

            return _ConsumerConfig;
        }

        /// <summary>
        /// Pick the default kafkanodes if consumer nodes are not explictly mentioned in clients config
        /// </summary>
        /// <returns></returns>
        public String GetConsumerKafkaNodes()
        {
            return _configuration["Kafka:KafkaConsumerNodes"] ?? _configuration["Kafka:KafkaNodes"];
        }

        /// <summary>
        /// Checks if secure connection is mentioned in client config
        /// </summary>
        /// <returns></returns>
        public bool IsConsumerSecured()
        {
            return Convert.ToBoolean((_configuration["Kafka:IsConsumerSecured"] ?? "false"));
        }

        /// <summary>
        /// Read ConnectionString if ConsumerConnectionString is not mentioned explicitly
        /// In case of 2 different event hubs are interacting we will need 2 different connection string
        /// </summary>
        /// <returns></returns>
        public bool IsEnableConsumerDeadLetter()
        {
            return Convert.ToBoolean((_configuration["Kafka:EnableConsumerDeadLetter"] ?? "false"));
        }

        public int GetTimeOut()
        {
            return Convert.ToInt32(_configuration["Kafka:TimeOut"] ?? "30000");
        }

        /// <summary>
        /// Read SaslMechanism config key. If the value is null/empty it will default to Plain
        /// </summary>
        /// <returns></returns>
        public SaslMechanism GetConsumerSaslMechanism()
        {
            return GetSaslMechanism(_configuration["Kafka:ConsumerSaslMechanism"] ?? _configuration["Kafka:SaslMechanism"]);
        }

        /// <summary>
        /// Flag to turn on/off EnableSSLCertificateVerification
        /// </summary>
        /// <returns></returns>
        public bool EnableSSLVerificationForConsumer()
        {
            return EnableSSLVerification(_configuration["Kafka:EnableSSLVerificationForConsumer"] ?? _configuration["Kafka:EnableSSLVerification"]);
        }

        /// <summary>
        /// Gets SaslUsername for consumer from configuration
        /// </summary>
        /// <returns></returns>
        public string GetConsumerSaslUsername()
        {
            return _configuration["Kafka:ConsumerSaslUsername"] ?? _configuration["Kafka:SaslUsername"];
        }

        /// <summary>
        /// Gets SaslPassword for consumer from configuration
        /// </summary>
        /// <returns></returns>
        private string GetConsumerSaslPassword()
        {
            return _configuration["Kafka:ConsumerSaslPassword"] ?? _configuration["Kafka:SaslPassword"];
        }

        /// <summary>
        /// Gets SslCaLocation cert location for consumer to connect.
        /// </summary>
        /// <returns></returns>
        private string GetConsumerSaslCaLocation()
        {
            return (_configuration["Kafka:ConsumerSslCaLocation"] ?? _configuration["Kafka:SslCaLocation"]) ?? string.Empty;
        }

        #endregion

        #region helper methods
        /// <summary>
        /// Read SaslMechanism / ProducerSaslMechanism / ConsumerSaslMechanism config key. If the value is null/empty it will default to Plain
        /// </summary>
        /// <returns></returns>
        private SaslMechanism GetSaslMechanism(string mechanismValue)
        {
            if (!Enum.TryParse(mechanismValue, true, out SaslMechanism _saslMechanism))
                _saslMechanism = SaslMechanism.Plain;

            return _saslMechanism;
        }

        /// <summary>
        /// Reads EnableSSLVerification / EnableSSLVerificationForProducer / EnableSSLVerificationForConsumer key from config file to determine if EnableSslCertificationValidation should be turned on/off.
        /// Default it to true if the value / key is not configured.
        /// </summary>
        /// <param name="useConnectionStringKey"></param>
        /// <returns></returns>
        private bool EnableSSLVerification(string enableSSLVerificationConfig)
        {
            if (string.IsNullOrEmpty(enableSSLVerificationConfig)) return true;

            if (Boolean.TryParse(enableSSLVerificationConfig, out var enableSSLVerification))
                return enableSSLVerification;
            else
                return false;
        }
        #endregion
    }
}
