using Nest;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Microsoft.CSharp.RuntimeBinder;

namespace Rickard.Butler.ElasticSearch
{
    public class ElasticSet<TDocumentType> where TDocumentType : class
    {
        private readonly string _index;
        private readonly Func<IndexSettingsDescriptor, IPromise<IIndexSettings>> _settings;
        private readonly Func<MappingsDescriptor, IPromise<IMappings>> _mapping;
        private ElasticClient _client { get; }

        public ElasticSet()
        {
        }

        public ElasticSet(string index, 
            ElasticClient client, 
            Func<IndexSettingsDescriptor, IPromise<IIndexSettings>> settings, 
            Func<MappingsDescriptor, IPromise<IMappings>> mapping)
        {
            _mapping = mapping;
            _settings = settings;
            _index = index;
           _client = client;
        }

        #region Get
        public TDocumentType GetById(string id)
        {
            var result = _client.Search<TDocumentType>(s => s
                .Index(_index)
                .Query(q => q
                    .Ids(c => c.Values(id))));

            return result.Documents.FirstOrDefault();
        }

        public async Task<TDocumentType> GetByIdAsync(string id)
        {
            var result = await _client.SearchAsync<TDocumentType>(s => s
                .Index(_index)
                .Query(q => q
                    .Ids(c => c.Values(id))));

            return result.Documents.FirstOrDefault();
        }

        public TDocumentType GetByIds(params string[] ids)
        {
            return GetByIdsInternal(ids);
        }

        public TDocumentType GetByIds(IEnumerable<string> ids)
        {
            return GetByIdsInternal(ids);
        }

        private TDocumentType GetByIdsInternal(IEnumerable<string> ids)
        {
            var result = _client.Search<TDocumentType>(s => s
                .Index(_index)
                .Query(q => q
                    .Ids(c => c.Values(ids))));

            return result.Documents.FirstOrDefault();
        }
        #endregion
        
        #region Add
        public void AddOrUpdate(TDocumentType document, bool waitForRefresh = true)
        {
            _client.Index(document, s => s.Index(_index).Refresh(waitForRefresh ? Refresh.True : Refresh.False));
        }
        public async Task AddOrUpdateAsync(TDocumentType document, bool waitForRefresh = true)
        {
            await _client.IndexAsync(document, s => s.Index(_index).Refresh(waitForRefresh ? Refresh.True : Refresh.False));
        }
        #endregion

        #region Update

        public void PartialUpdate<TPartialDocument>(string id, TPartialDocument partialDocument, bool waitForRefresh = true) where TPartialDocument : class
        {
            _client.Update<TDocumentType, TPartialDocument>(id, u => u
                .Index(_index)
                .Type(typeof(TDocumentType))
                .Doc(partialDocument)
                .Refresh(waitForRefresh ? Refresh.True : Refresh.False)
            );
        }

        public async Task PartialUpdateAsync<TPartialDocument>(string id, TPartialDocument partialDocument, bool waitForRefresh = true) where TPartialDocument : class
        {
            await _client.UpdateAsync<TDocumentType, TPartialDocument>(id, u => u
                .Index(_index)
                .Type(typeof(TDocumentType))
                .Doc(partialDocument)
                .Refresh(waitForRefresh ? Refresh.True : Refresh.False)
            );
        }

        #endregion

        #region Delete

        public void DeleteById(string id, bool waitForRefresh = true)
        {
            _client.Delete<TDocumentType>(id, d => d
                .Index(_index)
                .Type(typeof(TDocumentType))
                .Refresh(waitForRefresh ? Refresh.True : Refresh.False));
        }

        public async Task DeleteByIdAsync(string id, bool waitForRefresh = true)
        {
            await _client.DeleteAsync<TDocumentType>(id, d => d
                .Index(_index)
                .Type(typeof(TDocumentType))
                .Refresh(waitForRefresh ? Refresh.True : Refresh.False));
        }

        #endregion

        #region Index

        public void CreateIndex()
        {
            var existsResponse = _client.IndexExists(_index);
            if (!existsResponse.Exists)
            {
                var result = _client.CreateIndex(_index, c => c
                    .Settings(_settings)
                    .Mappings(_mapping));


                if (!result.IsValid)
                {
                    throw new Exception($"Failed to create index {_index} - {result.DebugInformation}");
                }
            }
        }

        public async Task CreateIndexAsync()
        {
            var existsResponse = await _client.IndexExistsAsync(_index);
            if (!existsResponse.Exists)
            {
                var result = await _client.CreateIndexAsync(_index, c => c
                    .Settings(_settings)
                    .Mappings(_mapping));

                if (!result.IsValid)
                {
                    throw new Exception($"Failed to create index {_index} - {result.DebugInformation}");
                }
            }
        }

        public void DeleteIndex()
        {
            var existsResponse = _client.IndexExists(_index);

            if (existsResponse.Exists)
            {
                var result = _client.DeleteIndex(_index);
                if (!result.IsValid)
                {
                    throw new Exception($"Failed to delete index {_index}");
                }
            }
        }

        public async Task DeleteIndexAsync()
        {
            var existsResponse = await _client.IndexExistsAsync(_index);

            if (existsResponse.Exists)
            {
                var result = await _client.DeleteIndexAsync(_index);
                if (!result.IsValid)
                {
                    throw new Exception($"Failed to delete index {_index}");
                }
            }
        }

        #endregion

        #region Search

        public IEnumerable<TDocumentType> Search_StartsWith(string search, Func<SortDescriptor<TDocumentType>, IPromise<IList<ISort>>> sortFieldExpr, params Expression<Func<TDocumentType, object>>[] fields)
        {
            var exps = new List<Func<QueryContainerDescriptor<TDocumentType>, QueryContainer>>();
            foreach (var field in fields)
            {
                exps.Add(e => e.MatchPhrasePrefix(m => m.Query(search).Field(field)));
            }

            var result = _client.Search<TDocumentType>(s => s
                .Sort(sortFieldExpr)
                .Index(_index)
                .Query(q => q.Bool(b => b.Should(exps))));

            return result.Documents;
        }

        public async Task<IEnumerable<TDocumentType>> Search_StartsWithAsync(string search, Func<SortDescriptor<TDocumentType>, IPromise<IList<ISort>>> sortFieldExpr, params Expression<Func<TDocumentType, object>>[] fields)
        {
            var exps = new List<Func<QueryContainerDescriptor<TDocumentType>, QueryContainer>>();
            foreach (var field in fields)
            {
                exps.Add(e => e.MatchPhrasePrefix(m => m.Query(search).Field(field)));
            }

            var result = await _client.SearchAsync<TDocumentType>(s => s
                .Sort(sortFieldExpr)
                .Index(_index)
                .Query(q => q.Bool(b => b.Should(exps))));

            return result.Documents;
        }

        public IEnumerable<TDocumentType> Search_ExactMatch(string search, params Expression<Func<TDocumentType, object>>[] fields)
        {
            var exps = new List<Func<QueryContainerDescriptor<TDocumentType>, QueryContainer>>();
            foreach (var field in fields)
            {
                exps.Add(e => e.MatchPhrase(m => m.Query(search).Field(field)));
            }

            var result = _client.Search<TDocumentType>(s => s
                .Index(_index)
                .Query(q => q.Bool(b => b.Should(exps))));

            return result.Documents;
        }

        public async Task<IEnumerable<TDocumentType>> Search_ExactMatchAsync(string search, params Expression<Func<TDocumentType, object>>[] fields)
        {
            var exps = new List<Func<QueryContainerDescriptor<TDocumentType>, QueryContainer>>();
            foreach (var field in fields)
            {
                exps.Add(e => e.MatchPhrase(m => m.Query(search).Field(field)));
            }

            var result = await _client.SearchAsync<TDocumentType>(s => s
                .Index(_index)
                .Query(q => q.Bool(b => b.Should(exps))));

            return result.Documents;
        }

        #endregion
    }
}
