using System;
using System.Collections.Generic;
using S2.BlackSwan.SupplyCollector.Models;
using Xunit;

namespace RedisSupplyCollectorTests
{
    public class RedisSupplyCollectorTests : IClassFixture<LaunchSettingsFixture>
    {
        private readonly RedisSupplyCollector.RedisSupplyCollector _instance;
        private readonly DataContainer _container;
        private LaunchSettingsFixture _fixture;

        public RedisSupplyCollectorTests(LaunchSettingsFixture fixture) {
            _fixture = fixture;
            _instance = new RedisSupplyCollector.RedisSupplyCollector();
            _container = new DataContainer()
            {
                ConnectionString = _instance.BuildConnectionString(
                    Environment.GetEnvironmentVariable("REDIS_HOST"),
                    Int32.Parse(Environment.GetEnvironmentVariable("REDIS_PORT")))
            };
        }

        [Fact]
        public void DataStoreTypesTest()
        {
            var result = _instance.DataStoreTypes();
            Assert.Contains("Redis", result);
        }

        [Fact]
        public void TestConnectionTest()
        {
            var result = _instance.TestConnection(_container);
            Assert.True(result);
        }

        [Fact]
        public void TestGetSchema() {
            var fields = new Dictionary<string, DataType>() {
                {"id", DataType.String},
                {"deleted", DataType.Boolean},
                {"created.user", DataType.String},
                {"created.date", DataType.DateTime},
                {"modified.user", DataType.String},
                {"modified.date", DataType.DateTime},
                {"modified.date_utc", DataType.DateTime},
                {"assigned_user_id", DataType.String},
                {"team_id", DataType.Unknown},
                {"name", DataType.String},
                {"start.date", DataType.DateTime},
                {"start.time", DataType.DateTime},
                {"parent.type", DataType.String},
                {"parent.id", DataType.String},
                {"description.text", DataType.String},
                {"description.html", DataType.Unknown},
                {"from.addr", DataType.String},
                {"from.name", DataType.String},
            };

            var (collections, entities) = _instance.GetSchema(new DataContainer(){ ConnectionString = $"{_container.ConnectionString}/key-collection-separator=:,key-levels=1,complex-values=true,complex-value-type=json"});
            
            Assert.Equal(1, collections.Count);
            Assert.Equal("emails", collections[0].Name);
            Assert.Equal(30, entities.Count);

            foreach (var field in fields)
            {
                var entity = entities.Find(x => x.Name.Equals(field.Key));
                Assert.True(entity!= null, $"Field {field.Key} exists");
                Assert.Equal(field.Value, entity.DataType);
            }
        }

        [Fact]
        public void GetMetricsTest()
        {
            var metrics = _instance.GetDataCollectionMetrics(new DataContainer() { ConnectionString = $"{_container.ConnectionString}/key-collection-separator=:,key-levels=1,complex-values=true,complex-value-type=json" });

            Assert.Equal(1, metrics.Count);
            Assert.Equal("emails", metrics[0].Name);
            Assert.Equal(200, metrics[0].RowCount);
        }

        [Fact]
        public void CollectSampleTest()
        {
            var container = new DataContainer() { ConnectionString = $"{_container.ConnectionString}/key-collection-separator=:,key-levels=1,complex-values=true,complex-value-type=json" };

            var entity = new DataEntity("from.addr", DataType.String, "string", container,
                new DataCollection(container, "emails"));

            var samples = _instance.CollectSample(entity, 10);
            Assert.Equal(10, samples.Count);
        }

    }
}
