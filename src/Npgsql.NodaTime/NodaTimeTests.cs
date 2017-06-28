using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NodaTime;
using NodaTime.Calendars;
using NpgsqlTypes;
using NUnit.Framework;

namespace Npgsql.NodaTime
{
    public class NodaTimeTests
    {
        [Test]
        public void Date()
        {
            using (var conn = OpenConnection())
            {
                var localDate = new LocalDate(2002, 3, 4);
                var localDateWithFiniteness = new LocalDateWithFiniteness(localDate);

                using (var cmd = new NpgsqlCommand("CREATE TEMP TABLE data (d1 DATE, d2 DATE, d3 DATE, d4 DATE, d5 DATE)", conn))
                    cmd.ExecuteNonQuery();

                using (var cmd = new NpgsqlCommand("INSERT INTO data VALUES (@p1, @p2, @p3, @p4, @p5)", conn))
                {
                    cmd.Parameters.Add(new NpgsqlParameter("p1", NpgsqlDbType.Date) { Value = localDate });
                    cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p2", Value = localDate });
                    cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p3", Value = LocalDateWithFiniteness.Infinity });
                    cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p4", Value = LocalDateWithFiniteness.NegativeInfinity });
                    cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p5", Value = new LocalDate(-5, 3, 3) });
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = new NpgsqlCommand("SELECT d1::TEXT, d2::TEXT, d3::TEXT, d4::TEXT, d5::TEXT FROM data", conn))
                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    Assert.That(reader.GetValue(0), Is.EqualTo("2002-03-04"));
                    Assert.That(reader.GetValue(1), Is.EqualTo("2002-03-04"));
                    Assert.That(reader.GetValue(2), Is.EqualTo("infinity"));
                    Assert.That(reader.GetValue(3), Is.EqualTo("-infinity"));
                    Assert.That(reader.GetValue(4), Is.EqualTo("0006-03-03 BC"));
                }

                using (var cmd = new NpgsqlCommand("SELECT * FROM data", conn))
                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();

                    // Regular type (LocalDate)
                    Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(LocalDate)));
                    Assert.That(reader.GetFieldValue<LocalDate>(0), Is.EqualTo(localDate));
                    Assert.That(reader.GetValue(0), Is.EqualTo(localDate));
                    Assert.That(() => reader.GetDateTime(0), Throws.TypeOf<InvalidCastException>());
                    Assert.That(() => reader.GetDate(0), Throws.TypeOf<InvalidCastException>());

                    // Provider-specific type (LocalDateWithFiniteness)
                    Assert.That(reader.GetProviderSpecificFieldType(0), Is.EqualTo(typeof(LocalDateWithFiniteness)));
                    Assert.That(reader.GetProviderSpecificValue(0), Is.EqualTo(localDateWithFiniteness));
                    Assert.That(reader.GetFieldValue<LocalDateWithFiniteness>(0), Is.EqualTo(localDateWithFiniteness));
                    // Skip the second, uninteresting
                    Assert.That(reader.GetFieldValue<LocalDateWithFiniteness>(2), Is.EqualTo(LocalDateWithFiniteness.Infinity));
                    Assert.That(reader.GetFieldValue<LocalDateWithFiniteness>(3), Is.EqualTo(LocalDateWithFiniteness.NegativeInfinity));
                    Assert.That(reader.GetFieldValue<LocalDate>(4), Is.EqualTo(new LocalDate(-5, 3, 3)));
                }
            }
        }

        [Test]
        public void Time()
        {
            using (var conn = OpenConnection())
            {
                var expected = new LocalTime(1, 2, 3, 4).PlusNanoseconds(5000);

                using (var cmd = new NpgsqlCommand("SELECT @p1, @p2", conn))
                {
                    cmd.Parameters.Add(new NpgsqlParameter("p1", NpgsqlDbType.Time) { Value = expected });
                    cmd.Parameters.Add(new NpgsqlParameter("p2", DbType.Time) { Value = expected });
                    using (var reader = cmd.ExecuteReader())
                    {
                        reader.Read();

                        for (var i = 0; i < cmd.Parameters.Count; i++)
                        {
                            Assert.That(reader.GetFieldType(i), Is.EqualTo(typeof(LocalTime)));
                            Assert.That(reader.GetFieldValue<LocalTime>(i), Is.EqualTo(expected));
                            Assert.That(reader.GetValue(i), Is.EqualTo(expected));
                            Assert.That(() => reader.GetTimeSpan(i), Throws.TypeOf<InvalidCastException>());
                        }
                    }
                }
            }
        }

        [Test]
        public void TimeTz()
        {
            using (var conn = OpenConnection())
            {
                var dateTime = new global::NodaTime.LocalDate() + new LocalTime(1, 2, 3, 4).PlusNanoseconds(5000);
                var offset = Offset.FromHoursAndMinutes(3, 30) + Offset.FromSeconds(5);
                var expected = new OffsetDateTime(dateTime, offset);

                using (var cmd = new NpgsqlCommand("SELECT @p", conn))
                {
                    cmd.Parameters.Add(new NpgsqlParameter("p", NpgsqlDbType.TimeTz) { Value = expected });
                    using (var reader = cmd.ExecuteReader())
                    {
                        reader.Read();

                        for (var i = 0; i < cmd.Parameters.Count; i++)
                        {
                            Assert.That(reader.GetFieldType(i), Is.EqualTo(typeof(OffsetDateTime)));
                            Assert.That(reader.GetFieldValue<OffsetDateTime>(i), Is.EqualTo(expected));
                            Assert.That(reader.GetValue(i), Is.EqualTo(expected));
                        }
                    }
                }
            }
        }

        [Test]
        public void TimeTzWithDateThrows()
        {
            using (var conn = OpenConnection())
            {
                var dateTime = new global::NodaTime.LocalDate(2017, 1, 1) + new LocalTime(1, 2, 3, 4).PlusNanoseconds(5000);
                var offset = Offset.FromHoursAndMinutes(3, 30) + Offset.FromSeconds(5);
                var expected = new OffsetDateTime(dateTime, offset);

                using (var cmd = new NpgsqlCommand("SELECT @p", conn))
                {
                    cmd.Parameters.Add(new NpgsqlParameter("p", NpgsqlDbType.TimeTz) { Value = expected });
                    Assert.That(() => cmd.ExecuteScalar(), Throws.TypeOf<InvalidCastException>());
                }
            }
        }

        [Test]
        public void Interval()
        {
            // Note: PG interval has microsecond precision, another under that is lost.
            var expected = new PeriodBuilder
            {
                Years = 1, Months = 2, Weeks = 3, Days = 4, Hours = 5, Minutes = 6, Seconds = 7,
                Milliseconds = 8, Nanoseconds = 9000
            }.Build().Normalize();
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand("SELECT @p1, @p2", conn))
            {
                cmd.Parameters.Add(new NpgsqlParameter("p1", NpgsqlDbType.Interval) { Value = expected });
                cmd.Parameters.AddWithValue("p2", expected);
                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();

                    for (var i = 0; i < cmd.Parameters.Count; i++)
                    {
                        Assert.That(reader.GetFieldType(i), Is.EqualTo(typeof(Period)));
                        Assert.That(reader.GetFieldValue<Period>(i), Is.EqualTo(expected));
                        Assert.That(reader.GetValue(i), Is.EqualTo(expected));
                        Assert.That(() => reader.GetTimeSpan(i), Throws.TypeOf<InvalidCastException>());
                    }
                }
            }
        }

        const string ConnectionString = "Host=localhost;Database=npgsql_tests;Username=npgsql_tests;Password=npgsql_tests";

        NpgsqlConnection OpenConnection()
        {
            var conn = new NpgsqlConnection(ConnectionString);
            conn.Open();
            conn.TypeMapper.UseNodatime();
            return conn;
        }
    }
}
