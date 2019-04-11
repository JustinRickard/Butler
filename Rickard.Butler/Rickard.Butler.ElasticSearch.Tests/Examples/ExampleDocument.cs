using System;
using System.Collections.Generic;
using System.Text;
using Nest;

namespace Rickard.Butler.ElasticSearch.Tests.Examples
{
    [ElasticsearchType(Name = "ExampleDocument", IdProperty = nameof(Id))]
    public class ExampleDocument
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
    }
}
