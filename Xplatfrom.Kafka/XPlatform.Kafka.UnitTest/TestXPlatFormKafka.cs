using Microsoft.Extensions.Configuration;
using Microsoft.VisualStudio.TestPlatform.ObjectModel.Client;
using NUnit.Framework;
using System;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Xplatform.Kafka;
using XPlatform.Kafka.UnitTest.Models;

namespace XPlatform.Kafka.UnitTest
{
    public class Tests
    {
        IConfiguration conf;
        Rootobject msg = new Rootobject();

        public static IConfigurationRoot GetIConfigurationRoot()
        {
            return new ConfigurationBuilder()
                .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: true)
                .Build();
        }

        [SetUp]
        public void Setup()
        {
            conf = (IConfiguration)GetIConfigurationRoot();
            msg.AmsObjectValetId = "a30dc561-32d4-422b-8269-48d63c8bf7ce";
            msg.SubmissionId = "92afe0d3-3389-4ab7-ab0c-d59813fffff1-TestDoc.docx";
            msg.ApplicationGuid = "50e069d5-6f10-444d-90b2-244016129076";
            msg.SystemGuid = "a6786f4f-c0f9-4fec-bae5-86c2156a420e";
            msg.SystemOfRecordContextKey = "A100129351";
            msg.ObjectClassGuid = "6d4fad09-88bc-4cee-8942-72dabec6c0cb";
            msg.Status = "Scanned";
            msg.OvId = 0;
            msg.ScannerId = "e176a2b069a34efab9f84f8d9769c9fa";
            msg.ScanTotalTimeinMSec = 2359;
            msg.Description = "Test Kafka Message ";
            msg.FileName = "Test Kafka File.txt";
            msg.UserId = "KafkaUnitTest";
        }

        [Test]
        public void TestKafkaProducer()
        {
            IKafkaConfig kconf = new KafkaConfig(conf);
            KafkaProducer<Rootobject> kafkaProducer = new KafkaProducer<Rootobject>(kconf, msg);
            kafkaProducer.ProduceMessage(msg);
        }

        [Test]
        public void TestKafkaConsumer()
        {
            Rootobject msg = new Rootobject();
            IKafkaConfig kconf = new KafkaConfig(conf);
            KProcessor worker = new KProcessor();
            KafkaConsumer<Rootobject> kafkaConsumer = new KafkaConsumer<Rootobject>(kconf, msg, worker);
            kafkaConsumer.ConsumeMessage();
        }
    }




}