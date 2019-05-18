using System;
using System.Collections.Generic;
using System.Text;

namespace Rickard.Butler.ElasticSearch
{
    public class ButlerElasticOptions
    {
        public string[] Uris { get; set; }
        public string DefaultIndex { get; set; }
        public int NumberOfShards { get; set; }
        public int NumberOfReplicas { get; set; }
        public string IndexPrefix { get; set; }
        public string IndexSuffix { get; set; }
    }
}
