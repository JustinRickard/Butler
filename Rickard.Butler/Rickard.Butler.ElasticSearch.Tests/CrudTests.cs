using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Elasticsearch.Net;
using FluentAssertions;
using Rickard.Butler.ElasticSearch.Tests.Examples;
using Xunit;

namespace Rickard.Butler.ElasticSearch.Tests
{
    public class CrudTests : TestBase
    {
        #region Insert and Get
        [Fact]
        public void ShouldInsertAndGetById()
        {
            Context.Examples.AddOrUpdate(ExampleDocA, Refresh.WaitFor);
            var doc = Context.Examples.GetById(ExampleDocA.Id);
            doc.Should().NotBeNull();
        }

        [Fact]
        public async Task ShouldInsertAndGetByIdAsync()
        {
            await Context.Examples.AddOrUpdateAsync(ExampleDocA, Refresh.WaitFor);
            var doc = await Context.Examples.GetByIdAsync(ExampleDocA.Id);
            doc.Should().NotBeNull();
        }

        [Fact]
        public void ShouldInsertAndGetByIds()
        {
            Context.Examples.AddOrUpdateMany(new List<ExampleDocument> { ExampleDocA, ExampleDocB }, Refresh.WaitFor);
            var docs = Context.Examples.GetByIds(ExampleDocA.Id, ExampleDocB.Id);
            docs.Count().Should().Be(2);
        }

        [Fact]
        public async Task ShouldInsertAndGetByIdsAsync()
        {
            await Context.Examples.AddOrUpdateManyAsync(new List<ExampleDocument> { ExampleDocA, ExampleDocB }, Refresh.WaitFor);
            var docs = await Context.Examples.GetByIdsAsync(ExampleDocA.Id, ExampleDocB.Id);
            docs.Count().Should().Be(2);
        }
        #endregion

        #region Update
        [Fact]
        public void ShouldUpdate()
        {
            Context.Examples.AddOrUpdate(ExampleDocA, Refresh.WaitFor);
            var doc = Context.Examples.GetById(ExampleDocA.Id);
            doc.Name.Should().Be(ExampleDocA.Name);

            var newName = "New name";
            doc.Name = newName;
            Context.Examples.AddOrUpdate(doc, Refresh.WaitFor);

            var updated = Context.Examples.GetById(ExampleDocA.Id);
            updated.Name.Should().Be(newName);
        }

        [Fact]
        public async Task ShouldUpdateAsync()
        {
            await Context.Examples.AddOrUpdateAsync(ExampleDocA, Refresh.WaitFor);
            var doc = await Context.Examples.GetByIdAsync(ExampleDocA.Id);
            doc.Name.Should().Be(ExampleDocA.Name);

            var newName = "New name";
            doc.Name = newName;
            Context.Examples.AddOrUpdate(doc, Refresh.WaitFor);

            var updated = Context.Examples.GetById(ExampleDocA.Id);
            updated.Name.Should().Be(newName);
        }
        #endregion

        #region Delete

        [Fact]
        public void ShouldDelete()
        {
            Context.Examples.AddOrUpdate(ExampleDocA, Refresh.WaitFor);
            var doc = Context.Examples.GetById(ExampleDocA.Id);
            doc.Should().NotBeNull();

            Context.Examples.DeleteById(doc.Id, Refresh.WaitFor);
            var docDeleted = Context.Examples.GetById(ExampleDocA.Id);
            docDeleted.Should().BeNull();
        }

        [Fact]
        public async Task ShouldDeleteAsync()
        {
            await Context.Examples.AddOrUpdateAsync(ExampleDocA, Refresh.WaitFor);
            var doc = await Context.Examples.GetByIdAsync(ExampleDocA.Id);
            doc.Should().NotBeNull();

            await Context.Examples.DeleteByIdAsync(doc.Id, Refresh.WaitFor);
            var docDeleted = await Context.Examples.GetByIdAsync(ExampleDocA.Id);
            docDeleted.Should().BeNull();
        }

        #endregion

    }
}
