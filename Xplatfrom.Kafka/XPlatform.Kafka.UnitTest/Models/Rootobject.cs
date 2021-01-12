using System;
using System.Collections.Generic;
using System.Text;

namespace XPlatform.Kafka.UnitTest.Models
{
    public class Rootobject
    {
        public string AmsObjectValetId { get; set; }
        public string SubmissionId { get; set; }
        public string ApplicationGuid { get; set; }
        public string SystemGuid { get; set; }
        public string SystemOfRecordContextKey { get; set; }
        public string ObjectClassGuid { get; set; }
        public string FileName { get; set; }
        public string UserId { get; set; }
        public string Description { get; set; }
        public DateTime SubmittedDate { get; set; }
        public string Status { get; set; }
        public int OvId { get; set; }
        public string ScannerId { get; set; }
        public int ScanTotalTimeinMSec { get; set; }
    }
}
