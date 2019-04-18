using System;
using System.Collections.Generic;
using System.Text;
using Nest;

namespace Rickard.Butler.ElasticSearch
{
    public static class ElasticExtensions
    {
        public static SearchResult<TDocumentType> ToSearchResult<TDocumentType>(this ISearchResponse<TDocumentType> rawResult) where TDocumentType : class
        {
            return new SearchResult<TDocumentType>
            {
                Result = rawResult.Documents,
                Total = rawResult.Total,
                RawResponse = rawResult
            };
        }
    }
}
