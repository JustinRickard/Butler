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
        public void ElasticContextExample()
        {
            var options = new ButlerElasticOptions {DefaultIndex = "examples", Uris = new[] {"http://localhost:9200"}};
            var ctx = new ExampleContext(Options.Create(options));
            var doc = GetExampleDocument();

            ctx.Examples.AddOrUpdate(doc);
            var addResult = ctx.Examples.GetById(doc.Id.ToString());
            addResult.Id.Should().Be(doc.Id);
            addResult.Name.Should().Be(doc.Name);

            var newName = "Example updated";
            ctx.Examples.AddOrUpdate(new ExampleDocument {Id = doc.Id, Name = newName });
            var updatedResult = ctx.Examples.GetById(doc.Id.ToString());
            updatedResult.Id.Should().Be(doc.Id);
            updatedResult.Name.Should().Be(newName);

            ctx.Examples.DeleteById(doc.Id.ToString());

            var resultAfterDelete = ctx.Examples.GetById(doc.Id.ToString());
            resultAfterDelete.Should().BeNull();
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
