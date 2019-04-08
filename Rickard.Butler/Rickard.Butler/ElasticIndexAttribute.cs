using System;
using System.Collections.Generic;
using System.Text;

namespace Rickard.Butler.ElasticSearch
{
    public class ElasticIndexAttribute : Attribute
    {
        public ElasticIndexAttribute(string value)
        {
            Value = value;
        }

        public string Value { get; set; }
    }
}
