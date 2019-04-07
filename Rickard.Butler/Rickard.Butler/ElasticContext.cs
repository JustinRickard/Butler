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

        public ElasticContext(IOptions<ButlerElasticOptions> options)
        {
            ConfigureClient(options.Value);
            InstantiateSets(options.Value);
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

        private void InstantiateSets(ButlerElasticOptions config)
        {
            var propertyNameToObjectMappings = new Dictionary<string, object>();

            var sets = this.GetType().GetProperties(); //.Where(p => p.PropertyType == typeof(ElasticSet<>));

            foreach (var set in sets)
            {
                var myType = typeof(ElasticSet<>);
                var type = set.PropertyType;
                Type[] typeParameters = type.GetGenericArguments();
                var constructedType = myType.MakeGenericType(typeParameters);
                var obj =  Activator.CreateInstance(constructedType, set.Name, Client);

                set.SetValue(this, obj, null);
            }
        }
    }
}
