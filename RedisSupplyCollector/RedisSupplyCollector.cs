using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;
using StackExchange.Redis;

namespace RedisSupplyCollector
{
    public class RedisSupplyCollector : SupplyCollectorBase {
        private string _keyPrefix = null;
        private string _keyCollectionSeparator = null;
        private int _keyLevels = 0;
        private bool _complexValues = false;
        private string _complexValueType = null;

        public override List<string> DataStoreTypes() {
            return (new[] { "Redis" }).ToList();
        }

        protected virtual void ParseConnectionStringAdditions(string additions)
        {
            _keyPrefix = null;
            _keyCollectionSeparator = ":";
            _keyLevels = 0;
            _complexValues = false;
            _complexValueType = "json";

            var parts = additions.Split(",");
            foreach (var part in parts)
            {
                if (String.IsNullOrEmpty(part))
                    continue;

                var pair = part.Split("=");
                if (pair.Length == 2)
                {
                    if ("key-prefix".Equals(pair[0]))
                    {
                        _keyPrefix = pair[1];
                    }
                    else if ("key-collection-separator".Equals(pair[0])) {
                        _keyCollectionSeparator = pair[1];
                    }
                    else if ("key-levels".Equals(pair[0])) {
                        _keyLevels = Int32.Parse(pair[1]);
                    }
                    else if ("complex-values".Equals(pair[0]))
                    {
                        _complexValues = Boolean.Parse(pair[1]);
                    }
                    else if ("complex-value-type".Equals(pair[0]))
                    {
                        _complexValueType = pair[1];
                    }
                }
            }
        }

        private string ParseConnectionString(string connectionString) {
            var index = connectionString.IndexOf("/");
            if (index > 0) {
                ParseConnectionStringAdditions(connectionString.Substring(index + 1));
                return connectionString.Substring(0, index);
            }

            return connectionString;
        }

        public string BuildConnectionString(string host, int port) {
            var options = new ConfigurationOptions();
            options.EndPoints.Add(host, port);
            return options.ToString();
        }

        private void FillObjectSamples(string prefix, JObject obj, string entityName, List<string> samples, int maxSamples, double probability) {
            var r = new Random();
            var properties = obj.Properties();
            foreach (var property in properties) {
                string name = $"{prefix}{property.Name}";
                if (name.Equals(entityName)) {
                    if (property.Value.Type == JTokenType.Array) {
                        var arr = (JArray)property.Value;
                        for (int i = 0; i < arr.Count; i++)
                        {
                            if(r.NextDouble() < probability)
                                samples.Add(arr[i].ToString());

                            if (samples.Count >= maxSamples)
                                return;
                        }
                    }
                    else {
                        if (r.NextDouble() < probability)
                            samples.Add(property.Value.ToString());
                    }

                    if (samples.Count >= maxSamples)
                        return;
                }
                else {
                    if (property.Value.Type == JTokenType.Array) {
                        var arr = (JArray)property.Value;
                        for (int i = 0; i < arr.Count; i++)
                        {
                            var arrayItem = arr[i];

                            if (arrayItem.Type == JTokenType.Object)
                            {
                                FillObjectSamples($"{prefix}{property.Name}.", (JObject)arrayItem, entityName, samples, maxSamples, probability);
                            }
                        }
                    } else if (property.Value.Type == JTokenType.Object) {
                        FillObjectSamples($"{prefix}{property.Name}.", (JObject)property.Value, entityName, samples, maxSamples, probability);
                    }
                }
            }
        }

        private long CalcRowCount(DataEntity dataEntity, IServer server) {
            long rowCount = 0;
            var keys = server.Keys(0, $"{_keyPrefix}{dataEntity.Collection.Name}*");
            foreach (var unused in keys)
                rowCount++;

            return rowCount;
        }

        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize) {
            var result = new List<string>();

            using (var redis =
                ConnectionMultiplexer.Connect(ConfigurationOptions.Parse(ParseConnectionString(dataEntity.Container.ConnectionString)))) {
                var db = redis.GetDatabase();

                if (_keyCollectionSeparator != null && _keyLevels > 0) {
                    var server = redis.GetServer(redis.GetEndPoints()[0]);

                    var rows = CalcRowCount(dataEntity, server);
                    double pct = 0.05 + (double)sampleSize / (rows <= 0 ? sampleSize : rows);
                    var r = new Random();

                    var keys = server.Keys(0, $"{_keyPrefix}{dataEntity.Collection.Name}*");
                    foreach (var key in keys) {
                        if (_complexValues) {
                            var value = db.StringGet(key);
                            if (!String.IsNullOrEmpty(value))
                            {
                                var obj = JObject.Parse(value);
                                FillObjectSamples("", obj, dataEntity.Name, result, sampleSize, pct);
                            }
                        }
                        else {
                            if(r.NextDouble() < pct)
                                result.Add(db.StringGet(key));
                        }

                        if (result.Count >= sampleSize)
                            break;
                    }
                }
                else {
                    if (_complexValues) {
                        if ("json".Equals(_complexValueType)) {
                            var value = db.StringGet(dataEntity.Collection.Name);
                            if (!String.IsNullOrEmpty(value)) {
                                var obj = JObject.Parse(value);
                                FillObjectSamples("", obj, dataEntity.Name, result, sampleSize, 1);
                            }
                        } else throw new ArgumentException($"Unknown complex value type {_complexValueType}");
                    }
                    else {
                        result.Add(db.StringGet(dataEntity.Name));
                    }
                }
            }

            return result;
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container) {
            var metrics = new List<DataCollectionMetrics>();

            using (var redis =
                ConnectionMultiplexer.Connect(
                    ConfigurationOptions.Parse(ParseConnectionString(container.ConnectionString)))) {
                var server = redis.GetServer(redis.GetEndPoints()[0]);
                
                var keys = server.Keys();

                DataCollectionMetrics metric = null;

                foreach (var key in keys) {
                    var keyString = key.ToString();

                    if (_keyPrefix != null) {
                        if (!keyString.StartsWith(_keyPrefix))
                            continue;
                    }

                    string collectionName = null;
                    if (_keyCollectionSeparator != null && _keyLevels > 0) {
                        var keyParts = keyString.Split(_keyCollectionSeparator);

                        var collectionNameBuilder = new StringBuilder();
                        for (int i = 0; i < _keyLevels && i < keyParts.Length; i++) {
                            if (i > 0)
                                collectionNameBuilder.Append(_keyCollectionSeparator);
                            collectionNameBuilder.Append(keyParts[i]);
                        }

                        collectionName = collectionNameBuilder.ToString();
                    }
                    else {
                        collectionName = keyString;
                    }

                    if (metric != null && metric.Name.Equals(collectionName)) {
                        metric.RowCount++;
                    }
                    else {
                        metric = metrics.Find(x => x.Name.Equals(collectionName));
                        if (metric == null) {
                            metric = new DataCollectionMetrics() {
                                Name = collectionName,
                                RowCount = 0
                            };
                            metrics.Add(metric);
                        }

                        metric.RowCount++;
                    }
                }
            }

            return metrics;
        }

        private void FillObjectEntities(DataContainer container, DataCollection collection, string prefix, JObject obj, List<DataEntity> entities)
        {
            var properties = obj.Properties();
            foreach (var property in properties)
            {
                if (entities.Find(x => x.Name.Equals($"{prefix}{property.Name}")) != null)
                {
                    continue;
                }

                switch (property.Value.Type)
                {
                    case JTokenType.Boolean:
                        entities.Add(new DataEntity($"{prefix}{property.Name}", DataType.Boolean, "Boolean", container, collection));
                        break;
                    case JTokenType.String:
                        entities.Add(new DataEntity($"{prefix}{property.Name}", DataType.String, "String", container, collection));
                        break;
                    case JTokenType.Date:
                        entities.Add(new DataEntity($"{prefix}{property.Name}", DataType.DateTime, "Date", container, collection));
                        break;
                    case JTokenType.Float:
                        entities.Add(new DataEntity($"{prefix}{property.Name}", DataType.Float, "Float", container, collection));
                        break;
                    case JTokenType.Integer:
                        entities.Add(new DataEntity($"{prefix}{property.Name}", DataType.Int, "Integer", container, collection));
                        break;
                    case JTokenType.Guid:
                        entities.Add(new DataEntity($"{prefix}{property.Name}", DataType.Guid, "Guid", container, collection));
                        break;
                    case JTokenType.Uri:
                        entities.Add(new DataEntity($"{prefix}{property.Name}", DataType.String, "Uri", container, collection));
                        break;
                    case JTokenType.Array:
                        entities.Add(new DataEntity($"{prefix}{property.Name}", DataType.String, "Array", container, collection));

                        var arr = (JArray)property.Value;
                        for (int i = 0; i < arr.Count; i++)
                        {
                            var arrayItem = arr[i];

                            if (arrayItem.Type == JTokenType.Object)
                            {
                                FillObjectEntities(container, collection, $"{prefix}{property.Name}.", (JObject)arrayItem, entities);
                            }
                        }

                        break;
                    case JTokenType.Object:
                        FillObjectEntities(container, collection, $"{prefix}{property.Name}.", (JObject)property.Value, entities);
                        break;
                    default:
                        entities.Add(new DataEntity($"{prefix}{property.Name}",
                            DataType.Unknown, Enum.GetName(typeof(JTokenType), property.Value.Type), container, collection));
                        break;
                }
            }
        }

        public override (List<DataCollection>, List<DataEntity>) GetSchema(DataContainer container) {
            var collections = new List<DataCollection>();
            var entities = new List<DataEntity>();

            var collectionKeys = new HashSet<string>();

            using (var redis =
                ConnectionMultiplexer.Connect(ConfigurationOptions.Parse(ParseConnectionString(container.ConnectionString))))
            {
                var server = redis.GetServer(redis.GetEndPoints()[0]);
                var db = redis.GetDatabase();

                var keys = server.Keys();
                foreach (var key in keys) {
                    var keyString = key.ToString();

                    if (_keyPrefix != null) {
                        if (!keyString.StartsWith(_keyPrefix))
                            continue;
                    }

                    if (_keyCollectionSeparator != null && _keyLevels > 0) {
                        var keyParts = keyString.Split(_keyCollectionSeparator);

                        var collectionName = new StringBuilder();
                        for (int i = 0; i < _keyLevels && i < keyParts.Length; i++) {
                            if (i > 0)
                                collectionName.Append(_keyCollectionSeparator);
                            collectionName.Append(keyParts[i]);
                        }

                        var keyEntityName = collectionName.Length == keyString.Length
                            ? keyString
                            : keyString.Substring(collectionName.Length + _keyCollectionSeparator.Length);

                        bool keyFound = false;
                        DataCollection collection;
                        if (collectionKeys.Contains(collectionName.ToString())) {
                            keyFound = true;
                            collection = collections.Find(x => x.Name.Equals(collectionName.ToString()));
                        }
                        else {
                            collection = new DataCollection(container, collectionName.ToString());
                            collections.Add(collection);
                            collectionKeys.Add(collection.Name);
                        }

                        if (_complexValues) {
                            if (!keyFound) {
                                if ("json".Equals(_complexValueType)) {
                                    var obj = JObject.Parse(db.StringGet(keyString));
                                    FillObjectEntities(container, collection, "", obj, entities);
                                }
                                else {
                                    throw new ArgumentException($"Unknown complex value type {_complexValueType}");
                                }
                            }
                        }
                        else {
                            entities.Add(new DataEntity(keyEntityName, DataType.String, "String", container, collection)); //TODO: detect, may be it's a list?
                        }
                    }
                    else {
                        collections.Add(new DataCollection(container, keyString));
                    }
                }
            }

            return (collections, entities);
        }

        public override bool TestConnection(DataContainer container) {
            try {
                using (var redis =
                    ConnectionMultiplexer.Connect(ConfigurationOptions.Parse(ParseConnectionString(container.ConnectionString)))) {
                    return redis.IsConnected;
                }
            }
            catch (Exception) {
                return false;
            }
        }
    }
}
