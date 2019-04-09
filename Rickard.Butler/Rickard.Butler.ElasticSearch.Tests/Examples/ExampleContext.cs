using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Options;
using Nest;

namespace Rickard.Butler.ElasticSearch.Tests.Examples
{
    public class ExampleContext : ElasticContext
    {
        private static class Indexes
        {
            public const string Examples = "examples";
        }

        public ExampleContext(IOptions<ButlerElasticOptions> options) : base(options)
        {
            IndexSettings = GetIndexSettings(options.Value);
            IndexMappings = GetIndexMappings(options.Value);

            Initialize(options.Value);
        }

        [ElasticIndex(Indexes.Examples)]
        public ElasticSet<ExampleDocument> Examples { get; set; }

        private Dictionary<string, Func<IndexSettingsDescriptor, IPromise<IIndexSettings>>> GetIndexSettings(ButlerElasticOptions options)
        {
            return new Dictionary<string, Func<IndexSettingsDescriptor, IPromise<IIndexSettings>>>
            {
                {Indexes.Examples, s => s.NumberOfShards(options.NumberOfShards).NumberOfReplicas(options.NumberOfReplicas) }
            };
        }

        private Dictionary<string, Func<MappingsDescriptor, IPromise<IMappings>>> GetIndexMappings(ButlerElasticOptions options)
        {
            return new Dictionary<string, Func<MappingsDescriptor, IPromise<IMappings>>>
            {
                {Indexes.Examples, m => m.Map<ExampleDocument>(mc => mc.AutoMap())}
            };
        }
    }
}
