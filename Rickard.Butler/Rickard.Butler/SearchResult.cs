using System;
using System.Collections.Generic;
using System.Text;
using Nest;

namespace Rickard.Butler.ElasticSearch
{
    public class SearchResult<TDocument> where TDocument : class
    {
        public IEnumerable<TDocument> Result { get; set; }
        public long Total { get; set; }
        public ISearchResponse<TDocument> RawResponse { get; set; }
    }
}
