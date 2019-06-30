using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Elasticsearch.Net;
using FluentAssertions;
using Nest;
using Rickard.Butler.ElasticSearch.Tests.Examples;
using Xunit;

namespace Rickard.Butler.ElasticSearch.Tests
{
    [Collection("Sequential")]
    public class SearchTests : TestBase
    {
        public SearchTests()
        {
            Context.Examples.AddOrUpdate(ExampleDocA, Refresh.True);
            Context.Examples.AddOrUpdate(ExampleDocB, Refresh.True);
            Context.Examples.AddOrUpdateMany(GenerateExampleDocs(8), Refresh.True);
        }

        #region Query Expression
        [Fact]
        public void ShouldFind_WithQueryExpression()
        {
            var result = Context.Examples.Search(DefaultSkip, DefaultTake,
                q => q.Bool(b => b.Should(s =>
                    s.MatchPhrase(m => m.Query(ExampleDocA.Id.ToString()).Field(f => f.Id)))));
            result.Total.Should().Be(1);
            result.Result.First().Name.Should().Be(ExampleDocA.Name);
        }

        [Fact]
        public async Task ShouldFind_WithQueryExpressionAsync()
        {
            var result = await Context.Examples.SearchAsync(DefaultSkip, DefaultTake,
                q => q.Bool(b => b.Should(s =>
                    s.MatchPhrase(m => m.Query(ExampleDocA.Id.ToString()).Field(f => f.Id)))));
            result.Total.Should().Be(1);
            result.Result.First().Name.Should().Be(ExampleDocA.Name);
        }
        #endregion

        [Fact]
        public void ShouldFind_StartsWith()
        {
            var searchResultsAsc = Context.Examples.Search_StartsWith("exam", DefaultSkip, DefaultTake, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name.Suffix(ExampleContext.Lowercase));
            searchResultsAsc.Result.Count().Should().Be(2);

            var searchResultsDesc = Context.Examples.Search_StartsWith("exam", DefaultSkip, DefaultTake, x => x.Descending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name.Suffix(ExampleContext.Lowercase));
            searchResultsDesc.Result.Count().Should().Be(2);

            var searchManyResults = Context.Examples.Search_StartsWith("generat", 0, 1000, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name.Suffix(ExampleContext.Lowercase), x => x.Description.Suffix(ExampleContext.Lowercase));
            searchManyResults.Result.Count().Should().Be(8);
        }

        [Fact]
        public async Task ShouldFind_StartsWithAsync()
        {
            var searchResultsAsc = await Context.Examples.Search_StartsWithAsync("exam", DefaultSkip, DefaultTake, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name.Suffix(ExampleContext.Lowercase));
            searchResultsAsc.Result.Count().Should().Be(2);

            var searchResultsDesc = await Context.Examples.Search_StartsWithAsync("exam", DefaultSkip, DefaultTake, x => x.Descending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name.Suffix(ExampleContext.Lowercase));
            searchResultsDesc.Result.Count().Should().Be(2);

            var searchManyResults = await Context.Examples.Search_StartsWithAsync("generat", 0, 1000, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name.Suffix(ExampleContext.Lowercase), x => x.Description.Suffix(ExampleContext.Lowercase));
            searchManyResults.Result.Count().Should().Be(8);
        }

        [Fact]
        public void ShouldOrderBy_StartsWith()
        {
            var searchNameAscResult = Context.Examples.Search_StartsWith("exam", DefaultSkip, DefaultTake, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name.Suffix(ExampleContext.Lowercase));
            searchNameAscResult.Result.ElementAt(0).Name.Should().Be(ExampleDocA.Name);
            searchNameAscResult.Result.ElementAt(1).Name.Should().Be(ExampleDocB.Name);

            var searchNameDescResults = Context.Examples.Search_StartsWith("exam", DefaultSkip, DefaultTake, x => x.Descending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name.Suffix(ExampleContext.Lowercase));
            searchNameDescResults.Result.ElementAt(0).Name.Should().Be(ExampleDocB.Name);
            searchNameDescResults.Result.ElementAt(1).Name.Should().Be(ExampleDocA.Name);

            var searchManyAscResults = Context.Examples.Search_StartsWith("gener", DefaultSkip, 1000, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name.Suffix(ExampleContext.Lowercase), x => x.Description.Suffix(ExampleContext.Lowercase));
            searchManyAscResults.Result.First().Name.Should().Be("Generated Example 1");
            searchManyAscResults.Result.Last().Name.Should().Be("Generated Example 8");

            var searchManyDescResults = Context.Examples.Search_StartsWith("gener", DefaultSkip, 1000, x => x.Descending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name.Suffix(ExampleContext.Lowercase), x => x.Description.Suffix(ExampleContext.Lowercase));
            searchManyDescResults.Result.First().Name.Should().Be("Generated Example 8");
            searchManyDescResults.Result.Last().Name.Should().Be("Generated Example 1");
        }
    }
}
