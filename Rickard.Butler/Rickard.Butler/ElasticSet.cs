using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Elasticsearch.Net;

namespace Rickard.Butler.ElasticSearch
{
    public class ElasticSet<TDocumentType> where TDocumentType : class
    {
        public readonly string Index;
        public readonly Type DocumentType;
        private readonly Func<IndexSettingsDescriptor, IPromise<IIndexSettings>> _settings;
        private readonly Func<MappingsDescriptor, IPromise<IMappings>> _mapping;
        private ElasticClient _client { get; }

        public ElasticSet()
        {
            DocumentType = typeof(TDocumentType);
        }

        public ElasticSet(string index, 
            ElasticClient client, 
            Func<IndexSettingsDescriptor, IPromise<IIndexSettings>> settings, 
            Func<MappingsDescriptor, IPromise<IMappings>> mapping)
        {
            _mapping = mapping;
            _settings = settings;
            Index = index;
           _client = client;
            DocumentType = typeof(TDocumentType);
        }

        #region Get

        public TDocumentType GetById(string id)
        {
            return GetByIdGeneric(c => c.Values(id));
        }

        public TDocumentType GetById(Guid id)
        {
            return GetByIdGeneric(c => c.Values(id));
        }

        public TDocumentType GetById(int id)
        {
            return GetByIdGeneric(c => c.Values(id));
        }

        public TDocumentType GetById(long id)
        {
            return GetByIdGeneric(c => c.Values(id));
        }

        private TDocumentType GetByIdGeneric(Func<IdsQueryDescriptor, IIdsQuery> idQuery)
        {
            var result = _client.Search<TDocumentType>(s => s
                .Index(Index)
                .Query(q => q
                    .Ids(idQuery)));

            return result.Documents.FirstOrDefault();
        }

        public async Task<TDocumentType> GetByIdAsync(string id)
        {
            return await GetByIdGenericAsync(c => c.Values(id));
        }

        public async Task<TDocumentType> GetByIdAsync(Guid id)
        {
            return await GetByIdGenericAsync(c => c.Values(id));
        }

        public async Task<TDocumentType> GetByIdAsync(int id)
        {
            return await GetByIdGenericAsync(c => c.Values(id));
        }

        public async Task<TDocumentType> GetByIdAsync(long id)
        {
            return await GetByIdGenericAsync(c => c.Values(id));
        }

        private async Task<TDocumentType> GetByIdGenericAsync(Func<IdsQueryDescriptor, IIdsQuery> idQuery)
        {
            var result = await _client.SearchAsync<TDocumentType>(s => s
                .Index(Index)
                .Query(q => q
                    .Ids(idQuery)));

            return result.Documents.FirstOrDefault();}


        public IEnumerable<TDocumentType> GetByIds(params string[] ids)
        {
            return GetByIdsGeneric(id => id.Values(ids));
        }

        public IEnumerable<TDocumentType> GetByIds(IEnumerable<string> ids)
        {
            return GetByIdsGeneric(id => id.Values(ids));
        }

        public IEnumerable<TDocumentType> GetByIds(params Guid[] ids)
        {
            return GetByIdsGeneric(id => id.Values(ids));
        }

        public IEnumerable<TDocumentType> GetByIds(IEnumerable<Guid> ids)
        {
            return GetByIdsGeneric(id => id.Values(ids));
        }

        public IEnumerable<TDocumentType> GetByIds(params long[] ids)
        {
            return GetByIdsGeneric(id => id.Values(ids));
        }

        public IEnumerable<TDocumentType> GetByIds(IEnumerable<long> ids)
        {
            return GetByIdsGeneric(id => id.Values(ids));
        }

        private IEnumerable<TDocumentType> GetByIdsGeneric(Func<IdsQueryDescriptor, IIdsQuery> idQuery)
        {
            var result = _client.Search<TDocumentType>(s => s
                .Index(Index)
                .Query(q => q
                    .Ids(idQuery)));

            return result.Documents;
        }

        public async Task<IEnumerable<TDocumentType>> GetByIdsAsync(params string[] ids)
        {
            return await GetByIdsGenericAsync(id => id.Values(ids));
        }

        public async Task<IEnumerable<TDocumentType>> GetByIdsAsync(IEnumerable<string> ids)
        {
            return await GetByIdsGenericAsync(id => id.Values(ids));
        }
        public async Task<IEnumerable<TDocumentType>> GetByIdsAsync(params Guid[] ids)
        {
            return await GetByIdsGenericAsync(id => id.Values(ids));
        }

        public async Task<IEnumerable<TDocumentType>> GetByIdsAsync(IEnumerable<Guid> ids)
        {
            return await GetByIdsGenericAsync(id => id.Values(ids));
        }

        public async Task<IEnumerable<TDocumentType>> GetByIdsAsync(params long[] ids)
        {
            return await GetByIdsGenericAsync(id => id.Values(ids));
        }

        public async Task<IEnumerable<TDocumentType>> GetByIdsAsync(IEnumerable<long> ids)
        {
            return await GetByIdsGenericAsync(id => id.Values(ids));
        }

        private async Task<IEnumerable<TDocumentType>> GetByIdsGenericAsync(Func<IdsQueryDescriptor, IIdsQuery> idQuery)
        {
            var result = await _client.SearchAsync<TDocumentType>(s => s
                .Index(Index)
                .Query(q => q
                    .Ids(idQuery)));

            return result.Documents;
        }

        #endregion

        #region Add
        public void AddOrUpdate(TDocumentType document, Refresh refresh = Refresh.False)
        {
            _client.Index(document, s => s.Index(Index).Refresh(refresh));
        }
        public async Task AddOrUpdateAsync(TDocumentType document, Refresh refresh = Refresh.False)
        {
            await _client.IndexAsync(document, s => s.Index(Index).Refresh(refresh));
        }

        public void AddOrUpdateMany(IEnumerable<TDocumentType> documents, Refresh refresh = Refresh.False)
        {
 
            _client.IndexMany(documents, Index, typeof(TDocumentType));
        }
        public async Task AddOrUpdateManyAsync(IEnumerable<TDocumentType> documents, Refresh refresh = Refresh.False)
        {
            await _client.IndexManyAsync(documents, Index, typeof(TDocumentType));
        }

        #endregion

        #region Update

        public void PartialUpdate<TPartialDocument>(string id, TPartialDocument partialDocument, Refresh refresh = Refresh.False) where TPartialDocument : class
        {
            _client.Update<TDocumentType, TPartialDocument>(id, u => u
                .Index(Index)
                .Doc(partialDocument)
                .Refresh(refresh)
            );
        }

        public async Task PartialUpdateAsync<TPartialDocument>(string id, TPartialDocument partialDocument, Refresh refresh = Refresh.False) where TPartialDocument : class
        {
            await _client.UpdateAsync<TDocumentType, TPartialDocument>(id, u => u
                .Index(Index)
                .Doc(partialDocument)
                .Refresh(refresh)
            );
        }

        #endregion

        #region Delete

        public void DeleteById(string id, Refresh refresh = Refresh.False)
        {
            _client.Delete<TDocumentType>(id, d => d
                .Index(Index)
                .Refresh(refresh));
        }

        public void DeleteById(Guid id, Refresh refresh = Refresh.False)
        {
            _client.Delete<TDocumentType>(id, d => d
                .Index(Index)
                .Refresh(refresh));
        }

        public void DeleteById(int id, Refresh refresh = Refresh.False)
        {
            _client.Delete<TDocumentType>(id, d => d
                .Index(Index)
                .Refresh(refresh));
        }

        public void DeleteById(long id, Refresh refresh = Refresh.False)
        {
            _client.Delete<TDocumentType>(id, d => d
                .Index(Index)
                .Refresh(refresh));
        }

        public async Task DeleteByIdAsync(string id, Refresh refresh = Refresh.False)
        {
            await _client.DeleteAsync<TDocumentType>(id, d => d
                .Index(Index)
                .Refresh(refresh));
        }

        public async Task DeleteByIdAsync(Guid id, Refresh refresh = Refresh.False)
        {
            await _client.DeleteAsync<TDocumentType>(id, d => d
                .Index(Index)
                .Refresh(refresh));
        }

        public async Task DeleteByIdAsync(int id, Refresh refresh = Refresh.False)
        {
            await _client.DeleteAsync<TDocumentType>(id, d => d
                .Index(Index)
                .Refresh(refresh));
        }

        public async Task DeleteByIdAsync(long id, Refresh refresh = Refresh.False)
        {
            await _client.DeleteAsync<TDocumentType>(id, d => d
                .Index(Index)
                .Refresh(refresh));
        }

        #endregion

        #region Index

        public void CreateIndex()
        {
            var existsResponse = _client.IndexExists(Index);
            if (!existsResponse.Exists)
            {
                var result = _client.CreateIndex(Index, c => c
                    .Settings(_settings)
                    .Mappings(_mapping));


                if (!result.IsValid && result.ServerError.Error.Type != "resource_already_exists_exception")
                {
                    throw new Exception($"Failed to create index {Index} - {result.DebugInformation}");
                }
            }
        }

        public async Task CreateIndexAsync()
        {
            var existsResponse = await _client.IndexExistsAsync(Index);
            if (!existsResponse.Exists)
            {
                var result = await _client.CreateIndexAsync(Index, c => c
                    .Settings(_settings)
                    .Mappings(_mapping));

                if (!result.IsValid && result.ServerError.Error.Type != "resource_already_exists_exception")
                {
                    throw new Exception($"Failed to create index {Index} - {result.DebugInformation}");
                }
            }
        }

        public void DeleteIndex()
        {
            var existsResponse = _client.IndexExists(Index);

            if (existsResponse.Exists)
            {
                var result = _client.DeleteIndex(Index);
                if (!result.IsValid)
                {
                    throw new Exception($"Failed to delete index {Index}");
                }
            }
        }

        public async Task DeleteIndexAsync()
        {
            var existsResponse = await _client.IndexExistsAsync(Index);

            if (existsResponse.Exists)
            {
                var result = await _client.DeleteIndexAsync(Index);
                if (!result.IsValid)
                {
                    throw new Exception($"Failed to delete index {Index}");
                }
            }
        }

        #endregion

        #region Search

        public SearchResult<TDocumentType> Search_StartsWith(string search, int skip, int take, Func<SortDescriptor<TDocumentType>, IPromise<IList<ISort>>> sortFieldExpr, params Expression<Func<TDocumentType, object>>[] fields)
        {
            var exps = new List<Func<QueryContainerDescriptor<TDocumentType>, QueryContainer>>();
            foreach (var field in fields)
            {
                exps.Add(e => e.MatchPhrasePrefix(m => m.Query(search).Field(field)));
            }

            var result = _client.Search<TDocumentType>(s => s
                .Size(take)
                .Skip(skip)
                .Sort(sortFieldExpr)
                .Index(Index)
                .Query(q => q.Bool(b => b.Should(exps))));

            return result.ToSearchResult();
        }

        public async Task<SearchResult<TDocumentType>> Search_StartsWithAsync(string search, int skip, int take, Func<SortDescriptor<TDocumentType>, IPromise<IList<ISort>>> sortFieldExpr, params Expression<Func<TDocumentType, object>>[] fields)
        {
            var exps = new List<Func<QueryContainerDescriptor<TDocumentType>, QueryContainer>>();
            foreach (var field in fields)
            {
                exps.Add(e => e.MatchPhrasePrefix(m => m.Query(search).Field(field)));
            }

            var result = await _client.SearchAsync<TDocumentType>(s => s
                .Size(take)
                .Skip(skip)
                .Sort(sortFieldExpr)
                .Index(Index)
                .Query(q => q.Bool(b => b.Should(exps))));

            return result.ToSearchResult();
        }

        public SearchResult<TDocumentType> Search(int skip, int take, Func<QueryContainerDescriptor<TDocumentType>, QueryContainer> searchExpression)
        {
            var result = _client.Search<TDocumentType>(s => s
                .Size(take)
                .Skip(skip)
                .Index(Index)
                .Query(searchExpression));

            return result.ToSearchResult();
        }

        public async Task<SearchResult<TDocumentType>> SearchAsync(int skip, int take, Func<QueryContainerDescriptor<TDocumentType>, QueryContainer> query)
        {
            var result = await _client.SearchAsync<TDocumentType>(s => s
                .Size(take)
                .Skip(skip)
                .Index(Index)
                .Query(query));

            return result.ToSearchResult();
        }

        public SearchResult<TDocumentType> Search(string search, int skip, int take, Func<SortDescriptor<TDocumentType>, IPromise<IList<ISort>>> sortFieldExpr, params Expression<Func<TDocumentType, object>>[] fields)
        {
            var exps = new List<Func<QueryContainerDescriptor<TDocumentType>, QueryContainer>>();
            foreach (var field in fields)
            {
                exps.Add(e => e.MatchPhrase(m => m.Query(search).Field(field)));
            }

            var result = _client.Search<TDocumentType>(s => s
                .Size(take)
                .Skip(skip)
                .Index(Index)
                .Query(q => q.Bool(b => b.Should(exps))));

            return result.ToSearchResult();
        }

        public async Task<SearchResult<TDocumentType>> SearchAsync(string search, int skip, int take, Func<SortDescriptor<TDocumentType>, IPromise<IList<ISort>>> sortFieldExpr, params Expression<Func<TDocumentType, object>>[] fields)
        {
            var exps = new List<Func<QueryContainerDescriptor<TDocumentType>, QueryContainer>>();
            foreach (var field in fields)
            {
                exps.Add(e => e.MatchPhrase(m => m.Query(search).Field(field)));
            }

            var result = await _client.SearchAsync<TDocumentType>(s => s
                .Size(take)
                .Skip(skip)
                .Index(Index)
                .Query(q => q.Bool(b => b.Should(exps))));

            return result.ToSearchResult();
        }

        public SearchResult<TDocumentType> SearchWildcard(string search, int skip, int take, Func<SortDescriptor<TDocumentType>, IPromise<IList<ISort>>> sortFieldExpr, Expression<Func<TDocumentType, object>> field)
        {
            var result = _client.Search<TDocumentType>(s => s
                .Size(take)
                .Skip(skip)
                .Index(Index)
                .Query(q => q.Wildcard(c => c
                    .Field(field)
                    .Value(search)
                )));

            return result.ToSearchResult();
        }

        public async Task<SearchResult<TDocumentType>> SearchWildcardAsync(string search, int skip, int take, Func<SortDescriptor<TDocumentType>, IPromise<IList<ISort>>> sortFieldExpr, Expression<Func<TDocumentType, object>> field)
        {
            var result = await _client.SearchAsync<TDocumentType>(s => s
                .Size(take)
                .Skip(skip)
                .Index(Index)
                .Query(q => q.Wildcard(c => c
                    .Field(field)
                    .Value(search)
                )));

            return result.ToSearchResult();
        }

        #endregion
    }
}
