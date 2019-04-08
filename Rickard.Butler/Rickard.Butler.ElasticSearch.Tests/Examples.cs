using System;
using System.Collections.Generic;
using System.Text;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Nest;
using Xunit;

namespace Rickard.Butler.ElasticSearch
{
    public class Examples
    {
        [Fact]
        private void ElasticContextExample()
        {
            var options = new ButlerElasticOptions {DefaultIndex = "examples", Uris = new[] {"http://localhost:9200"}};
            var ctx = new ExampleContext(Options.Create(options));

            var doc = GetExampleDocument();
            ctx.Examples.AddOrUpdate(doc);
            var result = ctx.Examples.GetById(doc.Id.ToString());

            result.Id.Should().Be(doc.Id);
        }

        private ExampleDocument GetExampleDocument()
        {
            return new ExampleDocument
            {
                Id = Guid.Parse("670847cf-029e-47cb-a0a1-d9c3c45d0b05"),
                Name = "Example"
            };

        }

        [ElasticsearchType(Name = "ExampleDocument", IdProperty = nameof(Id))]
        public class ExampleDocument
        {
            public Guid Id { get; set; }
            public string Name { get; set; }
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
