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
    public class ElasticSet<TDocumentType>
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
            var result = _client.Search<dynamic>(s => s
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
            var result = _client.Search<dynamic>(s => s
                .Index(_index)
                .Query(q => q
                    .Ids(c => c.Values(ids))));

            return result.Documents.FirstOrDefault();
        }
        #endregion

        #region Add
        public void Add(TDocumentType document)
        {
            _client.IndexDocument<dynamic>(document);
        }
        public async Task AddAsync(TDocumentType document)
        {
            await _client.IndexDocumentAsync<dynamic>(document);
        }
        #endregion

        #region Update
        public void Update(TDocumentType document)
        {
            _client.IndexDocument<dynamic>(document);
        }

        public async Task UpdateAsync(TDocumentType document)
        {
            await _client.IndexDocumentAsync<dynamic>(document);
        }

        public void PartialUpdate<TPartialDocument>(string id, TPartialDocument partialDocument)
        {
            _client.Update<dynamic, dynamic>(id, u => u
                .Index(_index)
                .Type(typeof(TDocumentType))
                .Doc(partialDocument)
            );
        }

        public async Task PartialUpdateAsync<TPartialDocument>(string id, TPartialDocument partialDocument)
        {
            await _client.UpdateAsync<dynamic, dynamic>(id, u => u
                .Index(_index)
                .Type(typeof(TDocumentType))
                .Doc(partialDocument)
            );
        }

        #endregion
    }
}
