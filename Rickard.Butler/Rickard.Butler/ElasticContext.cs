using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Elasticsearch.Net;
using Microsoft.Extensions.Options;
using Nest;
using Rickard.Butler.ElasticSearch;

namespace Rickard.Butler
{
    public class ElasticContext
    {
        public ElasticClient Client;
        protected Dictionary<string, Func<IndexSettingsDescriptor, IPromise<IIndexSettings>>> IndexSettings;
        protected Dictionary<string, Func<MappingsDescriptor, IPromise<IMappings>>> IndexMappings;
        protected ButlerElasticOptions ElasticOptions { get; set; }

        public ElasticContext(IOptions<ButlerElasticOptions> options)
        {
            ElasticOptions = options.Value;
            ConfigureClient(options.Value);
        }

        private void ConfigureClient(ButlerElasticOptions config)
        {
            if (config.Uris == null || !config.Uris.Any())
            {
                throw new Exception("No ElasticSearch URIs configured");
            }

            ConnectionSettings settings = null;

            if (config.Uris.Length > 1)
            {
                var connectionPool = new SniffingConnectionPool(config.Uris.Select(uri => new Uri(uri)));
                settings = new ConnectionSettings(connectionPool)
                    .DefaultIndex(config.DefaultIndex);
            }
            else
            {
                settings = new ConnectionSettings(new Uri(config.Uris.First()))
                    .DefaultIndex(config.DefaultIndex)
                    .DisableDirectStreaming();
            }

            SetDirectStreaming(settings, config);
            Client = new ElasticClient(settings);
        }

        protected void SetDirectStreaming(ConnectionSettings settings, ButlerElasticOptions options)
        {
            if (options.DisableDirectStreaming)
            {
                settings.DisableDirectStreaming();
            }
        }

        protected void Initialize(ButlerElasticOptions config)
        {
            var sets = this.GetType().GetProperties().Where(p => p.PropertyType.Name.StartsWith("ElasticSet"));

            foreach (var set in sets)
            {
                var type = set.PropertyType;
                var elasticSetType = typeof(ElasticSet<>);
                Type[] typeParameters = type.GetGenericArguments();
                var constructedType = elasticSetType.MakeGenericType(typeParameters);

                // Get index of ElasticIndex attribute or fall back to the property name on the ElasticContext
                string rawIndex = GetRawIndex(set);

                var obj = Activator.CreateInstance(constructedType, GetIndexWithPrefixAndSuffix(rawIndex), Client, GetIndexSettings(rawIndex), GetIndexMappings(rawIndex));
                set.SetValue(this, obj, null);

                var createIndexMethod = constructedType.GetMethod("CreateIndex");
                createIndexMethod.Invoke(obj, new object[]{ });
            }
        }

        private Func<IndexSettingsDescriptor, IPromise<IIndexSettings>> GetIndexSettings(string rawIndex)
        {
            //var index = GetIndexWithPrefixAndSuffix(rawIndex);
            var index = rawIndex;

            if (IndexSettings.ContainsKey(index))
            {
                return IndexSettings[index];
            }

            throw new Exception($"Could not get index settings for index {index}. Ensure this is configured in ElasticContext.");
        }

        private Func<MappingsDescriptor, IPromise<IMappings>> GetIndexMappings(string index)
        {
            if (IndexMappings.ContainsKey(index))
            {
                return IndexMappings[index];
            }

            throw new Exception($"Could not get index mappings for index {index}. Ensure this is configured in ElasticContext.");
        }


        private string GetRawIndex(PropertyInfo prop)
        {
            var indexAttribute = prop
                .GetCustomAttributes(typeof(ElasticIndexAttribute), true)
                .FirstOrDefault() as ElasticIndexAttribute;

            if (indexAttribute != null)
            {
                return indexAttribute.Value;
            }

            return  prop.Name.ToLower();
        }

        public string GetIndexWithPrefixAndSuffix(string rawIndexName)
        {
            return (ElasticOptions.IndexPrefix ?? string.Empty) +
                   rawIndexName +
                   (ElasticOptions.IndexSuffix ?? string.Empty);
        }
    }
}
