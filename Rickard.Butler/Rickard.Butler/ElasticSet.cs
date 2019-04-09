using Nest;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.CSharp.RuntimeBinder;

namespace Rickard.Butler.ElasticSearch
{
    public class ElasticSet<TDocumentType> where TDocumentType : class
    {
        private readonly string _index;
        private Func<IndexSettingsDescriptor, IPromise<IndexSettings>> _settings;
        private Func<MappingsDescriptor, IPromise<IMappings>> _mapping;
        private ElasticClient _client { get; }

        public ElasticSet()
        {
        }

        public ElasticSet(string index, 
            ElasticClient client, 
            Func<IndexSettingsDescriptor, IPromise<IndexSettings>> settings, 
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
        public void AddOrUpdate(TDocumentType document)
        {
            _client.IndexDocument(document);
        }
        public async Task AddOrUpdateAsync(TDocumentType document)
        {
            await _client.IndexDocumentAsync(document);
        }
        #endregion

        #region Update

        public void PartialUpdate<TPartialDocument>(string id, TPartialDocument partialDocument) where TPartialDocument : class
        {
            _client.Update<TDocumentType, TPartialDocument>(id, u => u
                .Index(_index)
                .Type(typeof(TDocumentType))
                .Doc(partialDocument)
            );
        }

        public async Task PartialUpdateAsync<TPartialDocument>(string id, TPartialDocument partialDocument) where TPartialDocument : class
        {
            await _client.UpdateAsync<TDocumentType, TPartialDocument>(id, u => u
                .Index(_index)
                .Type(typeof(TDocumentType))
                .Doc(partialDocument)
            );
        }

        #endregion

        #region Delete

        public void DeleteById(string id)
        {
            _client.Delete<TDocumentType>(id, d => d
                .Index(_index)
                .Type(typeof(TDocumentType)));
        }

        public async Task DeleteByIdAsync(string id)
        {
            await _client.DeleteAsync<TDocumentType>(id, d => d
                .Index(_index)
                .Type(typeof(TDocumentType)));
        }

        #endregion

        #region Index

        public void CreateIndex()
        {
            var result = _client.CreateIndex(_index, c => c
                .Settings(_settings)
                .Mappings(_mapping));

            if (!result.IsValid)
            {
                throw new Exception($"Failed to create index {_index} - {result.DebugInformation}");
            }
        }

        public async Task CreateIndexAsync()
        {
            var result = await _client.CreateIndexAsync(_index, c => c
                .Settings(_settings)
                .Mappings(_mapping));

            if (!result.IsValid)
            {
                throw new Exception($"Failed to create index {_index} - {result.DebugInformation}");
            }
        }

        public void DeleteIndex()
        {
            var result = _client.DeleteIndex(_index);
            if (!result.Acknowledged)
            {
                throw new Exception($"Failed to delete index {_index}");
            }
        }

        public async Task DeleteIndexAsync()
        {
            var result = await _client.DeleteIndexAsync(_index);
            if (!result.Acknowledged)
            {
                throw new Exception($"Failed to delete index {_index}");
            }
        }

        #endregion
    }
}
