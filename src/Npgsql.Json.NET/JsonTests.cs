using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NpgsqlTypes;
using NUnit.Framework;
using NUnit.Framework.Internal;

namespace Npgsql.Json.NET
{
    [Parallelizable(ParallelScope.None)]
    [TestFixture(NpgsqlDbType.Jsonb)]
    [TestFixture(NpgsqlDbType.Json)]
    public class JsonTests
    {
        const string ConnectionString = "Host=localhost;Database=npgsql_tests;Username=npgsql_tests;Password=npgsql_tests";

        // TODO: Implement AsObject: doing GetValue should give the deserialized

        [Test]
        public void RoundtripObject()
        {
            var expected = new Foo { Bar = 8 };
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand(@"SELECT @p", conn))
            {
                cmd.Parameters.Add(new NpgsqlParameter("p", _npgsqlDbType) { Value = expected });
                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    var actual = reader.GetFieldValue<Foo>(0);
                    Assert.That(actual.Bar, Is.EqualTo(8));
                }
            }
        }

        [Test]
        public void DeserializeFailure()
        {
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand($@"SELECT '[1, 2, 3]'::{_pgTypeName}", conn))
            using (var reader = cmd.ExecuteReader())
            {
                reader.Read();
                // Attempt to deserialize JSON array into object
                Assert.That(() => reader.GetFieldValue<Foo>(0), Throws.TypeOf<JsonSerializationException>());
                // State should still be OK to continue
                var actual = reader.GetFieldValue<JArray>(0);
                Assert.That((int)actual[0], Is.EqualTo(1));
            }
        }

        class Foo
        {
            public int Bar { get; set; }
        }

        [Test]
        public void RoundtripJObject()
        {
            var expected = new JObject { ["Bar"] = 8 };

            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand(@"SELECT @p", conn))
            {
                cmd.Parameters.Add(new NpgsqlParameter("p", _npgsqlDbType) { Value = expected });
                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    var actual = reader.GetFieldValue<JObject>(0);
                    Assert.That((int)actual["Bar"], Is.EqualTo(8));
                }
            }
        }

        [Test]
        public void RoundtripJArray()
        {
            var expected = new JArray(new[] { 1, 2, 3 });

            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand(@"SELECT @p", conn))
            {
                cmd.Parameters.Add(new NpgsqlParameter("p", _npgsqlDbType) { Value = expected });
                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    var jarray = reader.GetFieldValue<JArray>(0);
                    Assert.That(jarray.ToObject<int[]>(), Is.EqualTo(new[] { 1, 2, 3 }));
                }
            }
        }

        [Test]
        public void ClrTypeMapping()
        {
            var expected = new Foo { Bar = 8 };
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand(@"SELECT @p", conn))
            {
                conn.TypeMapper.UseJsonNet(new[] { typeof(Foo) });

                cmd.Parameters.AddWithValue("p", expected);
                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    var actual = reader.GetFieldValue<Foo>(0);
                    Assert.That(actual.Bar, Is.EqualTo(8));
                }
            }
        }

        [Test]
        public void RoundtripClrArray()
        {
            var expected = new[] { 1, 2, 3 };

            using (var conn = OpenConnection())
            {
                conn.TypeMapper.UseJsonNet(new[] { typeof(int[]) });

                using (var cmd = new NpgsqlCommand($@"SELECT @p::{_pgTypeName}", conn))
                {
                    cmd.Parameters.AddWithValue("p", expected);
                    using (var reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        var actual = reader.GetFieldValue<int[]>(0);
                        Assert.That(actual, Is.EqualTo(expected));
                    }
                }
            }
        }

        NpgsqlConnection OpenConnection()
        {
            var conn = new NpgsqlConnection(ConnectionString);
            conn.Open();
            conn.TypeMapper.UseJsonNet();
            return conn;
        }

        readonly NpgsqlDbType _npgsqlDbType;
        readonly string _pgTypeName;

        public JsonTests(NpgsqlDbType npgsqlDbType)
        {
            _npgsqlDbType = npgsqlDbType;
            _pgTypeName = npgsqlDbType.ToString().ToLower();
        }
    }

    public static class NpgsqlConnectionExtensions
    {
        public static int ExecuteNonQuery(this NpgsqlConnection conn, string sql, NpgsqlTransaction tx = null)
        {
            var cmd = tx == null ? new NpgsqlCommand(sql, conn) : new NpgsqlCommand(sql, conn, tx);
            using (cmd)
                return cmd.ExecuteNonQuery();
        }

        public static object ExecuteScalar(this NpgsqlConnection conn, string sql, NpgsqlTransaction tx = null)
        {
            var cmd = tx == null ? new NpgsqlCommand(sql, conn) : new NpgsqlCommand(sql, conn, tx);
            using (cmd)
                return cmd.ExecuteScalar();
        }

        public static async Task<int> ExecuteNonQueryAsync(this NpgsqlConnection conn, string sql, NpgsqlTransaction tx = null)
        {
            var cmd = tx == null ? new NpgsqlCommand(sql, conn) : new NpgsqlCommand(sql, conn, tx);
            using (cmd)
                return await cmd.ExecuteNonQueryAsync();
        }

        public static async Task<object> ExecuteScalarAsync(this NpgsqlConnection conn, string sql, NpgsqlTransaction tx = null)
        {
            var cmd = tx == null ? new NpgsqlCommand(sql, conn) : new NpgsqlCommand(sql, conn, tx);
            using (cmd)
                return await cmd.ExecuteScalarAsync();
        }
    }
}
