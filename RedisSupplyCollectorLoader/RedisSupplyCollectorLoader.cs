using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using S2.BlackSwan.SupplyCollector.Models;
using StackExchange.Redis;
using SupplyCollectorDataLoader;

namespace RedisSupplyCollectorLoader
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

        private string ConvertToJson(Dictionary<string, object> root) {
            var sb = new StringBuilder();

            sb.AppendLine("{");

            bool first = true;
            foreach (var keyValue in root) {
                if (!first) {
                    sb.AppendLine(",");
                }
                sb.Append($"\"{keyValue.Key}\": ");

                if (keyValue.Value is Dictionary<string, object> value) {
                    sb.AppendLine(ConvertToJson(value));
                }
                else if (keyValue.Value is int) {
                    sb.Append(keyValue.Value);
                }
                else if (keyValue.Value is double) {
                    sb.Append(keyValue.Value.ToString().Replace(",", "."));
                }
                else {
                    sb.Append($"\"{keyValue.Value}\"");
                }

                first = false;
            }

            sb.AppendLine("}");

            return sb.ToString();
        }


        public override void InitializeDatabase(DataContainer dataContainer) {
            // Do nothing
        }

        public override void LoadSamples(DataEntity[] dataEntities, long count) {
            using (var redis =
                ConnectionMultiplexer.Connect(
                    ConfigurationOptions.Parse(ParseConnectionString(dataEntities[0].Container.ConnectionString)))) {
                var db = redis.GetDatabase();
                Console.Write("Creating records: ");
                long rows = 0;
                while (rows < count) {
                    if ((rows % 1000) == 0) {
                        Console.Write(".");
                    }

                    var r = new Random();

                    if (_complexValues) {
                        var obj = new Dictionary<string, object>();

                        foreach (var dataEntity in dataEntities) {
                            var nameParts = dataEntity.Name.Split(".");

                            var curObj = obj;
                            for (int i = 0; i < nameParts.Length; i++) {
                                if (i == nameParts.Length - 1) {
                                    object value;
                                    switch (dataEntity.DataType) {
                                        case DataType.String:
                                            value = new Guid().ToString();
                                            break;
                                        case DataType.Int:
                                            value = r.Next();
                                            break;
                                        case DataType.Double:
                                            value = r.NextDouble();
                                            break;
                                        case DataType.Boolean:
                                            value = r.Next(100) > 50;
                                            break;
                                        case DataType.DateTime:
                                            value = DateTimeOffset
                                                .FromUnixTimeMilliseconds(
                                                    DateTimeOffset.Now.ToUnixTimeMilliseconds() + r.Next()).DateTime.ToString("O");
                                            break;
                                        default:
                                            value = r.Next();
                                            break;
                                    }

                                    curObj[nameParts[i]] = value;
                                }
                                else {
                                    if(!curObj.ContainsKey(nameParts[i]))
                                        curObj[nameParts[i]] = new Dictionary<string, object>();
                                }
                            }
                        }

                        db.StringSet($"{dataEntities[0].Collection.Name}{_keyCollectionSeparator}{rows}", ConvertToJson(obj));
                    }
                    else {
                        foreach (var dataEntity in dataEntities) {
                            switch (dataEntity.DataType)
                            {
                                case DataType.String:
                                    db.StringSet(
                                        $"{dataEntity.Collection.Name}{_keyCollectionSeparator}{dataEntity.Name}{_keyCollectionSeparator}{rows}",
                                        new Guid().ToString());
                                    break;
                                case DataType.Int:
                                    db.StringSet(
                                        $"{dataEntity.Collection.Name}{_keyCollectionSeparator}{dataEntity.Name}{_keyCollectionSeparator}{rows}",
                                        r.Next());
                                    break;
                                case DataType.Double:
                                    db.StringSet(
                                        $"{dataEntity.Collection.Name}{_keyCollectionSeparator}{dataEntity.Name}{_keyCollectionSeparator}{rows}",
                                        r.NextDouble());
                                    break;
                                case DataType.Boolean:
                                    db.StringSet(
                                        $"{dataEntity.Collection.Name}{_keyCollectionSeparator}{dataEntity.Name}{_keyCollectionSeparator}{rows}",
                                        r.Next(100) > 50);
                                    break;
                                case DataType.DateTime:
                                    var value = DateTimeOffset
                                        .FromUnixTimeMilliseconds(
                                            DateTimeOffset.Now.ToUnixTimeMilliseconds() + r.Next()).DateTime.ToString("O");

                                    db.StringSet(
                                        $"{dataEntity.Collection.Name}{_keyCollectionSeparator}{dataEntity.Name}{_keyCollectionSeparator}{rows}",
                                        value);
                                    break;
                                default:
                                    db.StringSet(
                                        $"{dataEntity.Collection.Name}{_keyCollectionSeparator}{dataEntity.Name}{_keyCollectionSeparator}{rows}",
                                        r.Next());
                                    break;
                            }
                        }
                    }

                    ++rows;
                }

                Console.WriteLine();
            }
        }

        public override void LoadUnitTestData(DataContainer dataContainer) {
            using (var redis =
                ConnectionMultiplexer.Connect(
                    ConfigurationOptions.Parse(ParseConnectionString(dataContainer.ConnectionString)))) {
                var db = redis.GetDatabase();

                using (var reader = new StreamReader("tests/emails-utf8.redis")) {
                    while (!reader.EndOfStream) {
                        var line = reader.ReadLine();
                        if (line != null) {
                            if(!line.StartsWith("SET "))
                                continue;

                            var ind1 = line.IndexOf(" ");
                            var ind2 = line.IndexOf(" ", ind1 + 1);

                            var key = line.Substring(ind1 + 2, ind2 - ind1 - 3);
                            var val = line.Substring(ind2 + 2, line.Length - ind2 - 3).Replace("\\\"", "\"");

                            db.StringSet(key, val);
                        }
                    }
                }
            }
        }
    }
}
