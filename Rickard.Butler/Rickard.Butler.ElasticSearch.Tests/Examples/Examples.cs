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
        private const int Skip = 0;
        private const int Take = 10;

        [Fact]
        public void ElasticContextExample()
        {
            // Set up Context and create index
            var options = GetOptions();
            var ctx = new ExampleContext(Options.Create(options));
            var doc = GetExampleDocument();
            try
            {
                // Insert
                ctx.Examples.AddOrUpdate(doc);
            var addResult = ctx.Examples.GetById(doc.Id.ToString());
            addResult.Id.Should().Be(doc.Id);
            addResult.Name.Should().Be(doc.Name);

            // Update
            var newName = "Example A updated";
            ctx.Examples.AddOrUpdate(new ExampleDocument {Id = doc.Id, Name = newName, Description = doc.Description});
            var updatedResult = ctx.Examples.GetById(doc.Id.ToString());
            updatedResult.Id.Should().Be(doc.Id);
            updatedResult.Name.Should().Be(newName);

            // Search - begins with
            var doc2 = GetExampleDocument2();
            ctx.Examples.AddOrUpdate(doc2);

            var searchResultsAsc = ctx.Examples.Search_StartsWith("Exam", Skip, Take, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name);
            searchResultsAsc.Result.Count().Should().Be(2);
            searchResultsAsc.Result.ElementAt(0).Name.Should().Be(newName);
            searchResultsAsc.Result.ElementAt(1).Name.Should().Be(doc2.Name);

            var searchResultsDesc = ctx.Examples.Search_StartsWith("Exam", Skip, Take, x => x.Descending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name);
            searchResultsDesc.Result.Count().Should().Be(2);
            searchResultsDesc.Result.ElementAt(0).Name.Should().Be(doc2.Name);
            searchResultsDesc.Result.ElementAt(1).Name.Should().Be(newName);

            var searchManyResults = ctx.Examples.Search_StartsWith("desc", Skip, Take, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name, x => x.Description);
            searchManyResults.Result.Count().Should().Be(2);

            // Search - exact
            var searchExactResults = ctx.Examples.Search("Desc", Skip, Take, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Description);
            searchExactResults.Result.Count().Should().Be(1);

            // Search - contains
            var searchContainsResults = ctx.Examples.Search("xamp", Skip, Take, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name, x => x.Description);
            searchContainsResults.Result.Count().Should().Be(2);
            var searchContainsResults2 = ctx.Examples.Search("esc", Skip, Take, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name, x => x.Description);
            searchContainsResults2.Result.Count().Should().Be(0); // Description is not using Ngram analyzer so can't search for contained text
            var searchWildcardResults = ctx.Examples.SearchWildcard("*esc*", Skip, Take, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Description);
            searchWildcardResults.Result.Count().Should().Be(2);

            // Delete
            ctx.Examples.DeleteById(doc.Id.ToString());
            var resultAfterDelete = ctx.Examples.GetById(doc.Id.ToString());
            resultAfterDelete.Should().BeNull();

            // Delete index
            ctx.Examples.DeleteIndex();
        }
        catch(Exception ex)
        {
                ctx.Examples.DeleteIndex();
                throw;
            }
        }

        [Fact]
        public async Task ElasticContextExampleAsync()
        {
            // Set up Context and create index
            var options = GetOptions();
            var ctx = new ExampleContext(Options.Create(options));
            var doc = GetExampleDocument();

            try
            {
                // Insert
                await ctx.Examples.AddOrUpdateAsync(doc);
                var addResult = await ctx.Examples.GetByIdAsync(doc.Id.ToString());
                addResult.Id.Should().Be(doc.Id);
                addResult.Name.Should().Be(doc.Name);

                // Update
                var newName = "Example A updated";
                await ctx.Examples.AddOrUpdateAsync(new ExampleDocument {Id = doc.Id, Name = newName, Description = doc.Description});
                var updatedResult = await ctx.Examples.GetByIdAsync(doc.Id.ToString());
                updatedResult.Id.Should().Be(doc.Id);
                updatedResult.Name.Should().Be(newName);

                // Search - begins with
                var doc2 = GetExampleDocument2();
                ctx.Examples.AddOrUpdate(doc2);
                var searchResultsAsc = (await ctx.Examples.Search_StartsWithAsync("Exam", Skip, Take, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name));
                searchResultsAsc.Result.Count().Should().Be(2);
                searchResultsAsc.Result.ElementAt(0).Name.Should().Be(newName);
                searchResultsAsc.Result.ElementAt(1).Name.Should().Be(doc2.Name);

                var searchResultsDesc = (await ctx.Examples.Search_StartsWithAsync("Exam", Skip, Take, x => x.Descending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name));
                searchResultsDesc.Result.Count().Should().Be(2);
                searchResultsDesc.Result.ElementAt(0).Name.Should().Be(doc2.Name);
                searchResultsDesc.Result.ElementAt(1).Name.Should().Be(newName);
                
                var searchManyResults = await ctx.Examples.Search_StartsWithAsync("desc", Skip, Take, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name, x => x.Description);
                searchManyResults.Result.Count().Should().Be(2);

                // Search - exact
                var searchExactResults = await ctx.Examples.SearchAsync("Desc", Skip, Take, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Description);
                searchExactResults.Result.Count().Should().Be(1);

                // Search - contains
                var searchContainsResults = await ctx.Examples.SearchAsync("xamp", Skip, Take, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name, x => x.Description);
                searchContainsResults.Result.Count().Should().Be(2);
                var searchContainsResults2 = await ctx.Examples.SearchAsync("esc", Skip, Take, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name, x => x.Description);
                searchContainsResults2.Result.Count().Should().Be(0); // Description is not using Ngram analyzer so can't search for contained text
                var searchWildcardResults = await ctx.Examples.SearchWildcardAsync("*esc*", Skip, Take, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Description);
                searchWildcardResults.Result.Count().Should().Be(2);

                // Delete
                await ctx.Examples.DeleteByIdAsync(doc.Id.ToString());
                var resultAfterDelete = await ctx.Examples.GetByIdAsync(doc.Id.ToString());
                resultAfterDelete.Should().BeNull();

                // Delete index
                await ctx.Examples.DeleteIndexAsync();
            }
            catch (Exception ex)
            {
                await ctx.Examples.DeleteIndexAsync();
                throw;
            }
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
                Name = "Example A",
                Description = "Desc"
            };
        }

        private ExampleDocument GetExampleDocument2()
        {
            return new ExampleDocument
            {
                Id = Guid.Parse("65265d38-44d5-49ae-b6d3-5fc18cd81939"),
                Name = "Example B",
                Description = "Desc2"
            };
        }
    }
}
