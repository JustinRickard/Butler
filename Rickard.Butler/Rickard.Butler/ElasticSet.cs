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
        private ElasticClient _client { get; }

        public ElasticSet()
        {
        }

        public ElasticSet(string index, ElasticClient client)
        {
           _index = index;
           _client = client;
        }

        #region GET
        public TDocumentType GetById(string id)
        {
            var result = _client.Search<TDocumentType>(s => s
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
    }
}
