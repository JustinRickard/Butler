using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using Nest;
using Rickard.Butler.ElasticSearch.Tests.Examples;
using Xunit;

namespace Rickard.Butler.ElasticSearch.Tests
{
    public class SearchTests : TestBase
    {
        public SearchTests()
        {
            Context.Examples.AddOrUpdate(ExampleDocA);
            Context.Examples.AddOrUpdate(ExampleDocB);
            Context.Examples.AddOrUpdateMany(GenerateExampleDocs(20));
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
            var searchResultsAsc = Context.Examples.Search_StartsWith("Exam", DefaultSkip, DefaultTake, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name);
            searchResultsAsc.Result.Count().Should().Be(2);

            var searchResultsDesc = Context.Examples.Search_StartsWith("Exam", DefaultSkip, DefaultTake, x => x.Descending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name);
            searchResultsDesc.Result.Count().Should().Be(2);

            var searchManyResults = Context.Examples.Search_StartsWith("desc", DefaultSkip, DefaultTake, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name, x => x.Description);
            searchManyResults.Result.Count().Should().Be(2);
        }

        [Fact]
        public void ShouldOrderBy_StartsWith()
        {
            var searchNameAscResult = Context.Examples.Search_StartsWith("Exam", DefaultSkip, DefaultTake, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name);
            searchNameAscResult.Result.ElementAt(0).Name.Should().Be(ExampleDocA.Name);
            searchNameAscResult.Result.ElementAt(1).Name.Should().Be(ExampleDocB.Name);

            var searchNameDescResults = Context.Examples.Search_StartsWith("Exam", DefaultSkip, DefaultTake, x => x.Descending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name);
            searchNameDescResults.Result.ElementAt(0).Name.Should().Be(ExampleDocB.Name);
            searchNameDescResults.Result.ElementAt(1).Name.Should().Be(ExampleDocA.Name);

            var searchManyAscResults = Context.Examples.Search_StartsWith("desc", DefaultSkip, DefaultTake, x => x.Ascending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name, x => x.Description);
            searchManyAscResults.Result.ElementAt(0).Name.Should().Be(ExampleDocA.Name);
            searchManyAscResults.Result.ElementAt(1).Name.Should().Be(ExampleDocB.Name);

            var searchManyDescResults = Context.Examples.Search_StartsWith("desc", DefaultSkip, DefaultTake, x => x.Descending(f => f.Name.Suffix(ExampleContext.Lowercase)), x => x.Name, x => x.Description);
            searchManyDescResults.Result.ElementAt(0).Name.Should().Be(ExampleDocB.Name);
            searchManyDescResults.Result.ElementAt(1).Name.Should().Be(ExampleDocA.Name);
        }
    }
}
