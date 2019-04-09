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

        public ElasticContext(IOptions<ButlerElasticOptions> options)
        {
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
                    .DefaultIndex(config.DefaultIndex);
            }

            Client = new ElasticClient(settings);
        }

        protected void Initialize(ButlerElasticOptions config)
        {
            var sets = this.GetType().GetProperties(); //.Where(p => p.PropertyType == typeof(ElasticSet<>));

            foreach (var set in sets)
            {
                var myType = typeof(ElasticSet<>);
                var type = set.PropertyType;
                Type[] typeParameters = type.GetGenericArguments();
                var constructedType = myType.MakeGenericType(typeParameters);

                // Get index of ElasticIndex attribute or fall back to the property name on the ElasticContext
                string index = GetIndex(set) ?? set.Name.ToLower();

                var obj = Activator.CreateInstance(constructedType, index, Client, GetIndexSettings(index), GetIndexMappings(index));
                set.SetValue(this, obj, null);

                var createIndexMethod = constructedType.GetMethod("CreateIndex");
                createIndexMethod.Invoke(obj, new object[]{ });
            }
        }

        private Func<IndexSettingsDescriptor, IPromise<IIndexSettings>> GetIndexSettings(string index)
        {
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


        private string GetIndex(PropertyInfo prop)
        {
            var indexAttribute = prop
                .GetCustomAttributes(typeof(ElasticIndexAttribute), true)
                .FirstOrDefault() as ElasticIndexAttribute;

            if (indexAttribute != null)
            {
                return indexAttribute.Value;
            }

            return null;
        }
    }
}
