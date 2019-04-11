using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Nest;
using Rickard.Butler.ElasticSearch.Tests.Examples;
using Xunit;

namespace Rickard.Butler.ElasticSearch.Examples
{
    // Below tests require ElasticSearch to be running on port 9200 (default port)
    public class Examples
    {
        [Fact]
        public void ElasticContextExample()
        {
            // Set up Context and create index
            var options = GetOptions();
            var ctx = new ExampleContext(Options.Create(options));
            var doc = GetExampleDocument();

            // Insert
            ctx.Examples.AddOrUpdate(doc);
            var addResult = ctx.Examples.GetById(doc.Id.ToString());
            addResult.Id.Should().Be(doc.Id);
            addResult.Name.Should().Be(doc.Name);

            // Update
            var newName = "Example updated";
            ctx.Examples.AddOrUpdate(new ExampleDocument {Id = doc.Id, Name = newName, Description = doc.Description });
            var updatedResult = ctx.Examples.GetById(doc.Id.ToString());
            updatedResult.Id.Should().Be(doc.Id);
            updatedResult.Name.Should().Be(newName);

            // Search - begins with
            ctx.Examples.AddOrUpdate(GetExampleDocument2());
            var searchResults = ctx.Examples.Search_StartsWith("Exam", x => x.Name);
            searchResults.Count().Should().Be(2);

            var searchManyResults = ctx.Examples.Search_StartsWith("desc", x => x.Name, x => x.Description);
            searchManyResults.Count().Should().Be(2);

            // Delete
            ctx.Examples.DeleteById(doc.Id.ToString());
            var resultAfterDelete = ctx.Examples.GetById(doc.Id.ToString());
            resultAfterDelete.Should().BeNull();

            // Delete index
            ctx.Examples.DeleteIndex();
        }

        [Fact]
        public async Task ElasticContextExampleAsync()
        {
            // Set up Context and create index
            var options = GetOptions();
            var ctx = new ExampleContext(Options.Create(options));
            var doc = GetExampleDocument();

            // Insert
            await ctx.Examples.AddOrUpdateAsync(doc);
            var addResult = await ctx.Examples.GetByIdAsync(doc.Id.ToString());
            addResult.Id.Should().Be(doc.Id);
            addResult.Name.Should().Be(doc.Name);

            // Update
            var newName = "Example updated";
            await ctx.Examples.AddOrUpdateAsync(new ExampleDocument { Id = doc.Id, Name = newName, Description = doc.Description });
            var updatedResult = await ctx.Examples.GetByIdAsync(doc.Id.ToString());
            updatedResult.Id.Should().Be(doc.Id);
            updatedResult.Name.Should().Be(newName);

            // Search - begins with
            ctx.Examples.AddOrUpdate(GetExampleDocument2());
            var searchResults = await ctx.Examples.Search_StartsWithAsync("Exam", x => x.Name);
            searchResults.Count().Should().Be(2);

            var searchManyResults = await ctx.Examples.Search_StartsWithAsync("desc", x => x.Name, x => x.Description);
            searchManyResults.Count().Should().Be(2);

            // Delete
            await ctx.Examples.DeleteByIdAsync(doc.Id.ToString());
            var resultAfterDelete = await ctx.Examples.GetByIdAsync(doc.Id.ToString());
            resultAfterDelete.Should().BeNull();

            // Delete index
            await ctx.Examples.DeleteIndexAsync();
        }

        private ButlerElasticOptions GetOptions()
        {
            return new ButlerElasticOptions
            {
                DefaultIndex = "default",
                Uris = new[] { "http://localhost:9200" },
                NumberOfReplicas = 0,
                NumberOfShards = 1
            };
        }

        private ExampleDocument GetExampleDocument()
        {
            return new ExampleDocument
            {
                Id = Guid.Parse("670847cf-029e-47cb-a0a1-d9c3c45d0b05"),
                Name = "Example",
                Description = "Desc"
            };
        }

        private ExampleDocument GetExampleDocument2()
        {
            return new ExampleDocument
            {
                Id = Guid.Parse("65265d38-44d5-49ae-b6d3-5fc18cd81939"),
                Name = "Example 2",
                Description = "Desc2"
            };
        }
    }
}
