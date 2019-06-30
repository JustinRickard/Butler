using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Options;
using Rickard.Butler.ElasticSearch.Tests.Examples;

namespace Rickard.Butler.ElasticSearch.Tests
{
    public abstract class TestBase : IDisposable
    {
        public TestBase()
        {
            Context = new ExampleContext(Options.Create(GetOptions()));
        }
        public void Dispose()
        {
            Context.Examples.DeleteIndex();
        }

        protected ExampleContext Context { get; set; }

        protected int DefaultSkip = 0;
        protected int DefaultTake = 10;

        protected ExampleDocument ExampleDocA = new ExampleDocument
        {
            Id = Guid.Parse("670847cf-029e-47cb-a0a1-d9c3c45d0b05"),
            Name = "Example A",
            Description = "Desc"
        };

        protected ExampleDocument ExampleDocB = new ExampleDocument
        {
            Id = Guid.Parse("65265d38-44d5-49ae-b6d3-5fc18cd81939"),
            Name = "Example B",
            Description = "Desc2"
        };

        protected ButlerElasticOptions GetOptions()
        {
            return new ButlerElasticOptions
            {
                DefaultIndex = "default",
                Uris = new[] { "http://localhost:9200" },
                NumberOfReplicas = 0,
                NumberOfShards = 1,
                IndexPrefix = "local-",
                IndexSuffix = "-tests"
            };
        }

        protected IEnumerable<ExampleDocument> GenerateExampleDocs(int count)
        {
            for (var i = 0; i < count; i++)
            {
                yield return new ExampleDocument
                {
                    Id = Guid.NewGuid(),
                    Description = $"Generated Description {i+1}",
                    Name = $"Generated Example {i+1}"
                };
            }
        }
    }
}
