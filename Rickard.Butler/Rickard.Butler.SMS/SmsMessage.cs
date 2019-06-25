using System;
using System.Collections.Generic;
using System.Text;

namespace Rickard.Butler.SMS
{
    public class SmsMessage
    {
        public string Body { get; set; }
        public string From { get; set; }
        public string To { get; set; }
    }
}
