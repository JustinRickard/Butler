using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Options;
using Nest;

namespace Rickard.Butler.ElasticSearch
{
    public class Examples
    {
        private void ElasticSetExample()
        {
            var client = new ElasticClient();
            var set = new ElasticSet<ExampleDocument>("index", client);

            var doc = set.GetById("id1");
            var docs1 = set.GetByIds("id1", "id2");
            var docs2 = set.GetByIds(new List<string>() { "id1", "id2" });
        }

        private void ElasticContextExample()
        {
            var options = new ButlerElasticOptions {DefaultIndex = "examples", Uris = new[] {"http://localhost:9200"}};
            var ctx = new ExampleContext(Options.Create(options));

            var doc = ctx.Examples.GetById("id1");
        }

        private class ExampleDocument
        {
        }

        private class ExampleContext : ElasticContext
        {
            public ExampleContext(IOptions<ButlerElasticOptions> options) : base(options)
            {
            }

            public ElasticSet<ExampleDocument> Examples { get; set; }
        }
    }
}
