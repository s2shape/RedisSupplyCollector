using System;
using System.Collections.Generic;
using System.Text;
using S2.BlackSwan.SupplyCollector.Models;
using StackExchange.Redis;
using SupplyCollectorDataLoader;

namespace RedisSupplyCollector
{
    public class RedisSupplyCollectorLoader : SupplyCollectorDataLoaderBase
    {
        private string _keyPrefix = null;
        private string _keyCollectionSeparator = null;
        private int _keyLevels = 0;
        private bool _complexValues = false;
        private string _complexValueType = null;

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
                    else if ("key-collection-separator".Equals(pair[0]))
                    {
                        _keyCollectionSeparator = pair[1];
                    }
                    else if ("key-levels".Equals(pair[0]))
                    {
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
        private string ParseConnectionString(string connectionString)
        {
            var index = connectionString.IndexOf("/");
            if (index > 0)
            {
                ParseConnectionStringAdditions(connectionString.Substring(index + 1));
                return connectionString.Substring(0, index);
            }

            return connectionString;
        }

        public override void LoadSamples(DataEntity dataEntity, long count) {
            using (var redis =
                ConnectionMultiplexer.Connect(
                    ConfigurationOptions.Parse(ParseConnectionString(dataEntity.Container.ConnectionString)))) {

                var db = redis.GetDatabase();

                Console.Write("Creating records: ");
                long rows = 0;
                while (rows < count) {
                    if ((rows % 1000) == 0) {
                        Console.Write(".");
                    }

                    var value = Guid.NewGuid().ToString();

                    if (_complexValues) {
                        var nameParts = dataEntity.Name.Split(".");
                        string json = null;
                        for (int i = nameParts.Length - 1; i >= 0; i--) {
                            if (json == null) {
                                json = "{\"" + nameParts[i] + "\": \"" + value + "\"}";
                            }
                            else {
                                json = "{\"" + nameParts[i] + "\": " + json + "}";
                            }
                        }

                        db.StringSet($"{dataEntity.Collection.Name}{_keyCollectionSeparator}{rows}", json);
                    }
                    else {
                        db.StringSet($"{dataEntity.Collection.Name}{_keyCollectionSeparator}{dataEntity.Name}{_keyCollectionSeparator}{rows}", value);
                    }

                    ++rows;
                }

                Console.WriteLine();
            }
        }
    }
}
